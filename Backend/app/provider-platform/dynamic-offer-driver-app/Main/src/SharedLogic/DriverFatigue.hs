{-
 Copyright 2022-23, Juspay India Pvt Ltd

 This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License

 as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. This program

 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY

 or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details. You should have received a copy of

 the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
-}

module SharedLogic.DriverFatigue
  ( -- * Config and state
    FatigueConfig (..),
    defaultFatigueConfig,
    FatigueState (..),

    -- * Pure fatigue maths
    applyRest,
    weightedRideMinutes,
    addRide,
    currentScore,
    dailyDrivingMinutesAt,

    -- * Level
    isBlockedAt,

    -- * Redis-backed API
    FatigueStatus (..),
    recordCompletedRide,
    getFatigueState,
    getFatigueStatus,
    isDriverFatigued,
    filterOutFatiguedDrivers,
  )
where

import Data.List (partition)
import qualified Data.Map.Strict as Map
import Data.Time (Day, UTCTime (..), utctDay)
import qualified Domain.Types.MerchantOperatingCity as DMOC
import Kernel.Prelude
import qualified Kernel.Storage.Hedis as Redis
import Kernel.Types.Id
import Kernel.Utils.Common

-- Per-driver FATIGUE GUARD with recovery by rest instead of a hard reset.
--
-- State: ONE JSON value per driver in Redis (so the pool filter reads a whole chunk with a single
-- pipelined MGET) holding a fatigue score in "minute-equivalents" as of the end of the last ride,
-- that ride's end time, and today's (local-day) driving seconds. Its TTL (36 h) only cleans up
-- idle drivers; it is NOT the break detector any more.
--
-- Maths (pure, below): the score decays exponentially over REST only, i.e. over the idle gap from
-- the previous ride's end to the next ride's start, never during a ride; so a long ride after a
-- short gap cannot look like a break. Each ride adds its minutes, weighted x1.5 in the local night
-- window. A 30-40 min rest after a long shift therefore only partially recovers, while a long rest
-- brings the score near zero. Daily driving does not decay; it resets per local day.
--
-- Level: a driver is blocked while the score is at the continuous-driving limit, while today's
-- driving is at the daily cap, or while still recovering from a block (see isBlockedAt).
--
-- All Redis calls go through withCrossAppRedis: rides end in the driver app while pooling runs in
-- the allocator (and batch 1 inline in the driver app), and all of them must see the same key.

------------------------------------------------------------------------------------------------
-- Config and state
------------------------------------------------------------------------------------------------

-- | Tunables. Production would move these to optional per-city TransporterConfig fields via
-- NammaDSL, falling back to these defaults.
data FatigueConfig = FatigueConfig
  { halfLifeMinutes :: Double,
    nightStartHour :: Int,
    nightEndHour :: Int,
    nightWeight :: Double,
    dailyCapMinutes :: Double,
    stateTtlSeconds :: Redis.ExpirationTime
  }
  deriving (Generic, Show, Eq)

defaultFatigueConfig :: FatigueConfig
defaultFatigueConfig =
  FatigueConfig
    { halfLifeMinutes = 60,
      nightStartHour = 22,
      nightEndHour = 6,
      nightWeight = 1.5,
      dailyCapMinutes = 600,
      stateTtlSeconds = 36 * 60 * 60
    }

data FatigueState = FatigueState
  { -- | Score in minute-equivalents, as of 'lastRideEndAt' (undecayed).
    fatigueScore :: Double,
    lastRideEndAt :: UTCTime,
    dailyDrivingSeconds :: Int,
    -- | Local day that 'dailyDrivingSeconds' and 'ridesToday' belong to.
    dailyDrivingDay :: Day,
    ridesToday :: Int
  }
  deriving (Generic, Show, Eq, FromJSON, ToJSON)

mkFatigueStateKey :: Text -> Text
mkFatigueStateKey driverId = "driver-offer:Fatigue:state:{" <> driverId <> "}"

------------------------------------------------------------------------------------------------
-- Pure fatigue maths (times are arguments, so these are unit-testable without Redis or waiting)
------------------------------------------------------------------------------------------------

toLocal :: Seconds -> UTCTime -> UTCTime
toLocal tz = addUTCTime (secondsToNominalDiffTime tz)

localDayOf :: Seconds -> UTCTime -> Day
localDayOf tz = utctDay . toLocal tz

localHourOf :: Seconds -> UTCTime -> Int
localHourOf tz t = floor (toRational (utctDayTime (toLocal tz t)) / 3600)

isNightHour :: FatigueConfig -> Int -> Bool
isNightHour cfg h
  | cfg.nightStartHour > cfg.nightEndHour = h >= cfg.nightStartHour || h < cfg.nightEndHour
  | otherwise = h >= cfg.nightStartHour && h < cfg.nightEndHour

-- | Exponential recovery over a rest period: every half-life of rest halves the score.
applyRest :: FatigueConfig -> NominalDiffTime -> Double -> Double
applyRest cfg rest score
  | rest <= 0 = score
  | otherwise = score * 0.5 ** ((realToFrac rest / 60) / cfg.halfLifeMinutes)

-- | Ride minutes weighted per local hour (night hours count 'nightWeight' times), split exactly at
-- local hour boundaries.
weightedRideMinutes :: FatigueConfig -> Seconds -> UTCTime -> UTCTime -> Double
weightedRideMinutes cfg tz start end = go start 0
  where
    go t acc
      | t >= end = acc
      | otherwise =
        let localT = toLocal tz t
            hour = localHourOf tz t
            localHourStart = UTCTime (utctDay localT) (fromIntegral (hour * 3600))
            nextBoundary = addUTCTime (negate (secondsToNominalDiffTime tz)) (addUTCTime 3600 localHourStart)
            segEnd = min end nextBoundary
            minutes = realToFrac (diffUTCTime segEnd t) / 60
            weight = if isNightHour cfg hour then cfg.nightWeight else 1
         in go segEnd (acc + weight * minutes)

-- | Fold one completed ride into the state. Decay applies only over the idle gap between the
-- previous ride's end and THIS ride's start; the ride itself then adds its weighted minutes.
addRide :: FatigueConfig -> Seconds -> UTCTime -> UTCTime -> Maybe FatigueState -> FatigueState
addRide cfg tz start end mbPrev =
  let rideSeconds = max 0 (round (diffUTCTime end start)) :: Int
      rideDay = localDayOf tz end
      added = weightedRideMinutes cfg tz start end
   in case mbPrev of
        Nothing ->
          FatigueState
            { fatigueScore = added,
              lastRideEndAt = end,
              dailyDrivingSeconds = rideSeconds,
              dailyDrivingDay = rideDay,
              ridesToday = 1
            }
        Just prev ->
          let idleGap = max 0 (diffUTCTime start prev.lastRideEndAt)
              sameDay = prev.dailyDrivingDay == rideDay
           in FatigueState
                { fatigueScore = applyRest cfg idleGap prev.fatigueScore + added,
                  lastRideEndAt = max end prev.lastRideEndAt,
                  dailyDrivingSeconds = rideSeconds + (if sameDay then prev.dailyDrivingSeconds else 0),
                  dailyDrivingDay = rideDay,
                  ridesToday = 1 + (if sameDay then prev.ridesToday else 0)
                }

-- | Score as of 'now': the stored score decayed over the rest since the last ride (read-only).
currentScore :: FatigueConfig -> UTCTime -> FatigueState -> Double
currentScore cfg now st = applyRest cfg (max 0 (diffUTCTime now st.lastRideEndAt)) st.fatigueScore

-- | Today's driving minutes as of 'now' (0 once the local day has rolled over).
dailyDrivingMinutesAt :: Seconds -> UTCTime -> FatigueState -> Double
dailyDrivingMinutesAt tz now st
  | localDayOf tz now == st.dailyDrivingDay = fromIntegral st.dailyDrivingSeconds / 60
  | otherwise = 0

------------------------------------------------------------------------------------------------
-- Level (built-in limits)
------------------------------------------------------------------------------------------------

blockScoreMinutes, releaseScoreMinutes :: Double
blockScoreMinutes = 240
releaseScoreMinutes = 120

-- | Blocked when the current score reaches 240 min-eq (~4 h of continuous day driving) or today's
-- driving reaches the daily cap. Hysteresis: once the last ride ended at >= 240, the driver stays
-- blocked until the score has halved to 120 (one half-life of rest); without it, a driver who
-- ended a ride at 250 would be unblocked after ~4 minutes.
isBlockedAt :: FatigueConfig -> Seconds -> UTCTime -> FatigueState -> Bool
isBlockedAt cfg tz now st =
  let score = currentScore cfg now st
   in score >= blockScoreMinutes
        || dailyDrivingMinutesAt tz now st >= cfg.dailyCapMinutes
        || (st.fatigueScore >= blockScoreMinutes && score >= releaseScoreMinutes)

------------------------------------------------------------------------------------------------
-- Redis-backed API
------------------------------------------------------------------------------------------------

getFatigueState :: (Redis.HedisFlow m r) => Id a -> m (Maybe FatigueState)
getFatigueState driverId = Redis.withCrossAppRedis $ Redis.get (mkFatigueStateKey driverId.getId)

-- | On ride end (after the end-ride transaction succeeded): fold the ride into the state. The ride
-- start falls back to end minus duration when tripStartTime is missing; zero-length rides are
-- ignored.
recordCompletedRide :: (Redis.HedisFlow m r) => Seconds -> Id a -> Maybe UTCTime -> UTCTime -> Int -> m ()
recordCompletedRide tz driverId mbStart end rideDurationSec = do
  let start = fromMaybe (addUTCTime (negate (fromIntegral rideDurationSec)) end) mbStart
  when (end > start) $ do
    prev <- getFatigueState driverId
    let st = addRide defaultFatigueConfig tz start end prev
    Redis.withCrossAppRedis $ Redis.setExp (mkFatigueStateKey driverId.getId) st defaultFatigueConfig.stateTtlSeconds

data FatigueStatus = FatigueStatus
  { fatigueScoreMinutes :: Double,
    dailyDrivingMinutes :: Double,
    blocked :: Bool,
    -- | Estimated minutes of rest until unblocked (5-minute steps up to 24 h); Nothing if longer.
    minutesUntilUnblocked :: Maybe Int
  }
  deriving (Generic, Show, Eq, FromJSON, ToJSON)

getFatigueStatus :: (Redis.HedisFlow m r) => Seconds -> Id DMOC.MerchantOperatingCity -> Id a -> m FatigueStatus
getFatigueStatus tz _merchantOpCityId driverId = do
  now <- getCurrentTime
  getFatigueState driverId <&> \case
    Nothing -> FatigueStatus 0 0 False (Just 0)
    Just st ->
      let blockedAt t = isBlockedAt defaultFatigueConfig tz t st
       in FatigueStatus
            { fatigueScoreMinutes = currentScore defaultFatigueConfig now st,
              dailyDrivingMinutes = dailyDrivingMinutesAt tz now st,
              blocked = blockedAt now,
              minutesUntilUnblocked = (* 5) <$> find (\k -> not (blockedAt (addUTCTime (fromIntegral (k * 300)) now))) ([0 .. 24 * 12] :: [Int])
            }

isDriverFatigued :: (Redis.HedisFlow m r) => Seconds -> Id DMOC.MerchantOperatingCity -> Id a -> m Bool
isDriverFatigued tz merchantOpCityId driverId = (.blocked) <$> getFatigueStatus tz merchantOpCityId driverId

-- | Drops blocked drivers from a pool chunk with a single pipelined MGET. Missing state and Redis
-- errors read as not blocked, so the guard fails open and never empties a pool on a Redis outage.
filterOutFatiguedDrivers ::
  (Redis.HedisFlow m r) =>
  Seconds ->
  Id DMOC.MerchantOperatingCity ->
  (d -> Id b) ->
  [d] ->
  m [d]
filterOutFatiguedDrivers _ _ _ [] = pure []
filterOutFatiguedDrivers tz _merchantOpCityId getDriverId drivers = do
  now <- getCurrentTime
  let keyFor d = mkFatigueStateKey (getDriverId d).getId
  statesByKey <- Map.fromList <$> Redis.withCrossAppRedis (Redis.mGetClusterWithKeys @FatigueState (map keyFor drivers))
  let isBlocked d = maybe False (isBlockedAt defaultFatigueConfig tz now) (Map.lookup (keyFor d) statesByKey)
      (blockedDrivers, kept) = partition isBlocked drivers
  unless (null blockedDrivers) $
    logInfo $ "DriverFatigue: filtered out " <> show (length blockedDrivers) <> " fatigued driver(s) from pool"
  pure kept

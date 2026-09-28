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

    -- * Levels (JSON Logic)
    FatigueLevel (..),
    FatigueDecision (..),
    FatigueRuleInput (..),
    mkRuleInput,
    defaultFatigueRule,
    fetchFatigueRules,
    evaluateFatigueLevel,

    -- * Redis-backed API
    FatigueStatus (..),
    recordCompletedRide,
    getFatigueState,
    getFatigueStatus,
    isDriverFatigued,
    filterOutFatiguedDrivers,
  )
where

import qualified Data.Aeson as A
import Data.List (partition)
import qualified Data.Map.Strict as Map
import Data.Time (Day, UTCTime (..), utctDay)
import qualified Domain.Types.MerchantOperatingCity as DMOC
import Kernel.Prelude
import qualified Kernel.Storage.Hedis as Redis
import Kernel.Types.Id
import Kernel.Utils.Common
import Lib.Yudhishthira.Tools.Utils (runLogics)

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
-- Levels: NONE / WARN / BLOCK are decided by JSON Logic rules (Yudhishthira's runLogics) over the
-- current score, daily minutes, time since the last ride etc. fetchFatigueRules is the single
-- seam for per-city rules (a future DRIVER_FATIGUE LogicDomain); today it returns the default
-- rule. A rule error is treated as NONE, so a bad rule can never block every driver.
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
-- Levels (JSON Logic via Yudhishthira)
------------------------------------------------------------------------------------------------

data FatigueLevel = NONE | WARN | BLOCK
  deriving (Generic, Show, Read, Eq, Ord, FromJSON, ToJSON)

data FatigueDecision = FatigueDecision
  { level :: FatigueLevel,
    reason :: Text
  }
  deriving (Generic, Show, Eq, FromJSON, ToJSON)

-- | What a fatigue rule sees.
data FatigueRuleInput = FatigueRuleInput
  { fatigueScoreMinutes :: Double,
    -- | Undecayed score at the end of the last ride = the peak before the current rest.
    scoreAtLastRideEndMinutes :: Double,
    dailyDrivingMinutes :: Double,
    dailyCapMinutes :: Double,
    minutesSinceLastRide :: Double,
    halfLifeMinutes :: Double,
    localHour :: Int,
    isNight :: Bool,
    ridesToday :: Int
  }
  deriving (Generic, Show, Eq, FromJSON, ToJSON)

mkRuleInput :: FatigueConfig -> Seconds -> UTCTime -> FatigueState -> FatigueRuleInput
mkRuleInput cfg tz now st =
  let hour = localHourOf tz now
      sameDay = localDayOf tz now == st.dailyDrivingDay
   in FatigueRuleInput
        { fatigueScoreMinutes = currentScore cfg now st,
          scoreAtLastRideEndMinutes = st.fatigueScore,
          dailyDrivingMinutes = dailyDrivingMinutesAt tz now st,
          dailyCapMinutes = cfg.dailyCapMinutes,
          minutesSinceLastRide = max 0 (realToFrac (diffUTCTime now st.lastRideEndAt) / 60),
          halfLifeMinutes = cfg.halfLifeMinutes,
          localHour = hour,
          isNight = isNightHour cfg hour,
          ridesToday = if sameDay then st.ridesToday else 0
        }

-- | Default rule (JSON Logic), used when a city has no rules of its own:
--
--   * BLOCK when today's driving reaches the daily cap (does not recover by resting),
--   * BLOCK when the score reaches 240 min-eq (~4 h of continuous day driving),
--   * BLOCK while recovering from a block: the last ride ended at >= 240 and the score has not yet
--     halved to 120, i.e. at least one half-life (~60 min at the default) of rest. Without this
--     hysteresis a driver who ended a ride at 250 would be unblocked after ~4 minutes of rest,
--   * WARN from 180 (~3 h),
--   * otherwise NONE.
--
-- 4 h / 60 min is in line with common driving-hours rules (e.g. a 45 min break after 4.5 h of
-- driving); a stricter city only needs a different rule, not a code change.
--
-- Written as nested 3-argument "if"s: the json-logic-hs engine behind runLogics does not accept the
-- multi-branch form ("if": [c1, a, c2, b, ..., else]).
defaultFatigueRule :: A.Value
defaultFatigueRule =
  ifThenElse
    (A.object ["or" A..= [ge "dailyDrivingMinutes" (A.object ["var" A..= ("dailyCapMinutes" :: Text)]), ge "fatigueScoreMinutes" (A.Number 240)]])
    (decision "BLOCK" "continuous or daily driving limit reached")
    $ ifThenElse
      (A.object ["and" A..= [ge "scoreAtLastRideEndMinutes" (A.Number 240), ge "fatigueScoreMinutes" (A.Number 120)]])
      (decision "BLOCK" "recovering from a driving-limit block")
      $ ifThenElse
        (ge "fatigueScoreMinutes" (A.Number 180))
        (decision "WARN" "approaching the continuous driving limit")
        (decision "NONE" "rested")
  where
    ifThenElse :: A.Value -> A.Value -> A.Value -> A.Value
    ifThenElse cond thenV elseV = A.object ["if" A..= [cond, thenV, elseV]]
    ge :: Text -> A.Value -> A.Value
    ge var threshold = A.object [">=" A..= [A.object ["var" A..= var], threshold]]
    decision :: Text -> Text -> A.Value
    decision lvl why = A.object ["level" A..= lvl, "reason" A..= why]

-- | The single seam for per-city rules. Switching to a DRIVER_FATIGUE LogicDomain means fetching
-- via Tools.DynamicLogic.getAppDynamicLogic here and falling back to the default when empty.
fetchFatigueRules :: Applicative m => Id DMOC.MerchantOperatingCity -> UTCTime -> m [A.Value]
fetchFatigueRules _merchantOpCityId _localTime = pure [defaultFatigueRule]

-- | Runs the rules with Yudhishthira's engine. Any rule error, or an output that is not a
-- {level, reason} object, is logged and treated as NONE.
evaluateFatigueLevel :: (MonadFlow m) => [A.Value] -> FatigueRuleInput -> m FatigueDecision
evaluateFatigueLevel rules input = do
  resp <- runLogics rules input
  case (resp.errors, A.fromJSON resp.result) of
    ([], A.Success decision) -> pure decision
    (errs, parsed) -> do
      let why = case parsed of
            A.Error parseErr -> ", unparseable output: " <> toText parseErr
            A.Success _ -> ""
      logError $ "DriverFatigue: fatigue rule failed, treating as NONE. errors=" <> show errs <> why
      pure $ FatigueDecision NONE "rule error"

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
    level :: FatigueLevel,
    reason :: Text,
    -- | Estimated minutes of rest until the level is NONE (re-running the rules on the projected
    -- decay, in 5-minute steps up to 24 h); Nothing if it would take longer.
    minutesUntilNone :: Maybe Int
  }
  deriving (Generic, Show, Eq, FromJSON, ToJSON)

decideAt :: (MonadFlow m) => [A.Value] -> Seconds -> UTCTime -> FatigueState -> m FatigueDecision
decideAt rules tz now st = evaluateFatigueLevel rules (mkRuleInput defaultFatigueConfig tz now st)

getFatigueStatus :: (MonadFlow m, Redis.HedisFlow m r) => Seconds -> Id DMOC.MerchantOperatingCity -> Id a -> m FatigueStatus
getFatigueStatus tz merchantOpCityId driverId = do
  now <- getCurrentTime
  rules <- fetchFatigueRules merchantOpCityId (toLocal tz now)
  getFatigueState driverId >>= \case
    Nothing -> pure $ FatigueStatus 0 0 NONE "no recent rides" (Just 0)
    Just st -> do
      decision <- decideAt rules tz now st
      let input = mkRuleInput defaultFatigueConfig tz now st
          stepsOf5Min = [0 .. 24 * 12] :: [Int]
      untilNone <-
        if decision.level == NONE
          then pure (Just 0)
          else findM (\k -> (== NONE) . (.level) <$> decideAt rules tz (addUTCTime (fromIntegral (k * 300)) now) st) stepsOf5Min <&> fmap (* 5)
      pure
        FatigueStatus
          { fatigueScoreMinutes = input.fatigueScoreMinutes,
            dailyDrivingMinutes = input.dailyDrivingMinutes,
            level = decision.level,
            reason = decision.reason,
            minutesUntilNone = untilNone
          }
  where
    findM _ [] = pure Nothing
    findM p (x : xs) = p x >>= \ok -> if ok then pure (Just x) else findM p xs

isDriverFatigued :: (MonadFlow m, Redis.HedisFlow m r) => Seconds -> Id DMOC.MerchantOperatingCity -> Id a -> m Bool
isDriverFatigued tz merchantOpCityId driverId = (== BLOCK) . (.level) <$> getFatigueStatus tz merchantOpCityId driverId

-- | Drops BLOCK-level drivers from a pool chunk: one pipelined MGET for the whole chunk, then the
-- rules are evaluated per driver in-process. WARN drivers are kept and logged (future: FCM nudge).
-- Missing state, Redis errors and rule errors all read as NONE, so the guard fails open.
filterOutFatiguedDrivers ::
  (MonadFlow m, Redis.HedisFlow m r) =>
  Seconds ->
  Id DMOC.MerchantOperatingCity ->
  (d -> Id b) ->
  [d] ->
  m [d]
filterOutFatiguedDrivers _ _ _ [] = pure []
filterOutFatiguedDrivers tz merchantOpCityId getDriverId drivers = do
  now <- getCurrentTime
  let keyFor d = mkFatigueStateKey (getDriverId d).getId
  statesByKey <- Map.fromList <$> Redis.withCrossAppRedis (Redis.mGetClusterWithKeys @FatigueState (map keyFor drivers))
  rules <- fetchFatigueRules merchantOpCityId (toLocal tz now)
  decided <- forM drivers $ \d -> case Map.lookup (keyFor d) statesByKey of
    Nothing -> pure (d, NONE)
    Just st -> (d,) . (.level) <$> decideAt rules tz now st
  let (blocked, kept) = partition ((== BLOCK) . snd) decided
      warned = filter ((== WARN) . snd) kept
  unless (null blocked) $
    logInfo $ "DriverFatigue: filtered out " <> show (length blocked) <> " fatigued driver(s) from pool"
  unless (null warned) $
    logInfo $ "DriverFatigue: " <> show (length warned) <> " driver(s) at WARN level kept in pool"
  pure (map fst kept)

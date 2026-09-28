{-
 Copyright 2022-23, Juspay India Pvt Ltd

 This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License

 as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. This program

 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY

 or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details. You should have received a copy of

 the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
-}

module SharedLogic.DriverFatigue
  ( FatigueStatus (..),
    maxContinuousDrivingSeconds,
    mandatoryBreakSeconds,
    recordCompletedRide,
    isDriverFatigued,
    getFatigueStatus,
    filterOutFatiguedDrivers,
  )
where

import Data.List (partition)
import qualified Data.Map.Strict as Map
import Kernel.Prelude
import qualified Kernel.Storage.Hedis as Redis
import Kernel.Types.Id
import Kernel.Utils.Common

-- Per-driver FATIGUE GUARD = seconds of trip time driven without a break. One Redis counter per
-- driver: every completed ride INCRBYs its duration and resets the key's TTL to the mandatory
-- break length. If no ride completes for a full break, Redis expires the key (break taken, counter
-- back to zero), so no cron job or table is needed. Drivers at or over the limit are filtered out
-- of new driver pools; since they get no new rides, nothing refreshes the TTL and they unblock
-- themselves once the break has elapsed. All calls go through withCrossAppRedis because rides end
-- in the driver app while pooling runs in the allocator, and both must read the same key.
mkContinuousDrivingKey :: Text -> Text
mkContinuousDrivingKey driverId = "driver-offer:Fatigue:continuousDrivingSec:{" <> driverId <> "}"

-- Hardcoded for now; production would move these to optional per-city TransporterConfig fields
-- via NammaDSL (falling back to these defaults when unset).
maxContinuousDrivingSeconds :: Int
maxContinuousDrivingSeconds = 4 * 60 * 60

mandatoryBreakSeconds :: Redis.ExpirationTime
mandatoryBreakSeconds = 20 * 60

data FatigueStatus = FatigueStatus
  { continuousDrivingSeconds :: Int,
    limitSeconds :: Int,
    fatigued :: Bool,
    eligibleAgainInSeconds :: Int
  }
  deriving (Generic, Show, Eq, FromJSON, ToJSON)

-- On ride completion: add the trip duration and restart the break window.
recordCompletedRide :: (Redis.HedisFlow m r) => Id a -> Int -> m ()
recordCompletedRide driverId rideDurationSec =
  when (rideDurationSec > 0) $
    Redis.withCrossAppRedis $ do
      let key = mkContinuousDrivingKey driverId.getId
      void $ Redis.incrby key (fromIntegral rideDurationSec)
      Redis.expire key mandatoryBreakSeconds

getContinuousDrivingSeconds :: (Redis.HedisFlow m r) => Id a -> m Int
getContinuousDrivingSeconds driverId =
  Redis.withCrossAppRedis $ fromMaybe 0 <$> Redis.get (mkContinuousDrivingKey driverId.getId)

isDriverFatigued :: (Redis.HedisFlow m r) => Id a -> m Bool
isDriverFatigued driverId = (>= maxContinuousDrivingSeconds) <$> getContinuousDrivingSeconds driverId

getFatigueStatus :: (Redis.HedisFlow m r) => Id a -> m FatigueStatus
getFatigueStatus driverId = do
  drivenSec <- getContinuousDrivingSeconds driverId
  let isFatigued = drivenSec >= maxContinuousDrivingSeconds
  -- TTL is -2 for a missing key and -1 for one without expiry; both clamp to 0.
  remainingBreakSec <-
    if isFatigued
      then max 0 . fromIntegral <$> Redis.withCrossAppRedis (Redis.ttl (mkContinuousDrivingKey driverId.getId))
      else pure 0
  pure
    FatigueStatus
      { continuousDrivingSeconds = drivenSec,
        limitSeconds = maxContinuousDrivingSeconds,
        fatigued = isFatigued,
        eligibleAgainInSeconds = remainingBreakSec
      }

-- Drops fatigued drivers from a pool chunk with a single pipelined MGET. Missing keys and Redis
-- errors read as 0, so the guard fails open and never empties a pool on a Redis outage.
filterOutFatiguedDrivers :: (Redis.HedisFlow m r) => (d -> Id b) -> [d] -> m [d]
filterOutFatiguedDrivers _ [] = pure []
filterOutFatiguedDrivers getDriverId drivers = do
  let keyFor d = mkContinuousDrivingKey (getDriverId d).getId
  drivenByKey <- Map.fromList <$> Redis.withCrossAppRedis (Redis.mGetClusterWithKeys @Int (map keyFor drivers))
  let (fatiguedDrivers, restedDrivers) = partition (\d -> Map.findWithDefault 0 (keyFor d) drivenByKey >= maxContinuousDrivingSeconds) drivers
  unless (null fatiguedDrivers) $
    logInfo $ "DriverFatigue: filtered out " <> show (length fatiguedDrivers) <> " fatigued driver(s) from pool"
  pure restedDrivers

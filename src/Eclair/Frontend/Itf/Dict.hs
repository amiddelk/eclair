{-# LANGUAGE TypeFamilies, FlexibleInstances, UndecidableInstances, GADTs, EmptyDataDecls #-}
module Eclair.Frontend.Itf.Dict where

import Data.Map(Map)
import Eclair.Frontend.Base
import Eclair.Frontend.Itf.Base


-- * Interface for map objects

class IsObj o => IsDictObj o where
  type DictKey o
  type DictValue o

class IsDictObj o => HasLookup o where
  lookup :: Obj o -> DictKey o -> Maybe (DictValue o)

class IsDictObj o => HasUpdateBinding o where
  updateBinding :: DictKey o -> DictValue o -> Obj o -> Obj o

class IsDictObj o => HasHideBinding o where
  hideBinding :: DictKey o -> Obj o -> Obj o


-- | Interface for non-deletable map
class (IsDictObj o, HasLookup o, HasUpdateBinding o, HasWrap o, HasView o) => IsDict o
instance (IsDictObj o, HasLookup o, HasUpdateBinding o, HasWrap o, HasView o, k ~ DictKey o, ObjType o ~ Map k (DictValue o), Ord k) => IsDict o

-- | Placeholder for the key and value
data IDict k v

-- | Type index for dictonaries
data TDict t where
  TDict :: TDict (IDict k v)

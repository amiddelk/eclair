{-# LANGUAGE TypeFamilies, FlexibleInstances, UndecidableInstances, GADTs #-}
module Eclair.Frontend.Itf.Integer where

import Eclair.Frontend.Base
import Eclair.Frontend.Itf.Base

-- | Interface for incrementable objects (a.k.a. counters).
class IsObj o => HasIncr o where
  incr :: (IsStore s, ObjStore o ~ s) => Obj o -> Obj o


-- | Interface of a counter object.
class (HasIncr o, HasWrap o, HasView o) => IsCounter o
instance (Num a, ObjType o ~ a, HasIncr o, HasWrap o, HasView o) => IsCounter o

-- type index for shared integers
data TCounter t where
  TCounter        :: TCounter t
  TCounterInt     :: TCounter Int
  TCounterInteger :: TCounter Integer

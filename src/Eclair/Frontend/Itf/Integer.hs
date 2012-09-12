{-# LANGUAGE TypeFamilies, FlexibleInstances, UndecidableInstances #-}
module Eclair.Frontend.Itf.Integer where

import Eclair.Frontend.Base
import Eclair.Frontend.Itf.Base

-- | Interface for incrementable objects.
class IsObj o => HasIncr o where
  incr :: (IsStore s, ObjStore o ~ s) => Obj o -> Obj o


-- | Interface of an Int object.
class (HasIncr o, HasWrap o, HasView o) => IsInt o where
  intTypeDict :: ObjTypeDict o Int

instance (ObjType o ~ Int, HasIncr o, HasWrap o, HasView o) => IsInt o where
  intTypeDict = ObjTypeDict

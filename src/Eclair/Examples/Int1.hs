module Eclair.Examples.Int1 where

import Eclair.Frontend
import Eclair.Backend.Reference



genericRun s = do
  {-
  -- create some integer
  r <- transactionally s $ do
    ctx <- getCtx
    let obj = wrap ctx (0 :: Int)
    return ()
    -- create obj
  -}

  return ()


main :: IO ()
main = do
  s <- initStore
  genericRun s

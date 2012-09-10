{-# LANGUAGE TypeSynonymInstances #-}
-- Version vectors with exceptions.

module VersionVector (emptyVV, record, includes, merge, compareVV,VV,TS) where

import Data.Map as Map
import Data.List as List
import Data.Number.PartialOrd

type Interval = (Int,Int)
type VV a = Map a [Interval]
type TS a = (a,Int)

emptyVV :: VV a
emptyVV = Map.empty

record :: Ord a =>  TS a -> VV a -> VV a
record (s,t) vv = 
  let intervals = findWithDefault [(0,0)] s vv
      addTS vv   = case vv of
        (from,to):is  | from <= t && t <= to -> (from,to)   : is
                      | t == to+1            -> (from,to+1) : is                   
                      | otherwise            -> case is of
                            []               -> (from,to):[(t,t)]
                            (next,_):_   | t < next  -> (from,to) : (t,t) : is
                                         | otherwise -> (from,to) : addTS is
  in Map.insert s (compact $ addTS intervals) vv

compact :: [Interval] -> [Interval]
compact intervals =
  case intervals of
       [(from,to)] -> [(from,to)]
       (from1,to1):(from2,to2):rest -> if to1 == from2 || to1 + 1 == from2 
                                        then compact ((from1,to2):rest) 
                                        else (from1,to1) : compact ((from2,to2):rest)

includes :: Ord a => TS a -> VV a -> Bool
includes (s,t) vv =
  let intervals = findWithDefault [(0,0)] s vv
  in List.any (\(from,to) -> from <= t && t <= to) intervals

instance Ord a => PartialOrd (VV a) where
  cmp = compareVV

compareVV :: Ord a => VV a -> VV a -> Maybe Ordering
compareVV vv1 vv2 | vv1 == vv2  = Just EQ
                  | v2Dominates = Just LT
                  | v1Dominates = Just GT
                  | otherwise   = Nothing

  where v2Dominates = isSubmapOfBy includedIn vv1 vv2 -- v2 dominates v1 (v1 < v2)
        v1Dominates = isSubmapOfBy includedIn vv2 vv1 -- v1 dominates v2 (v2 < v1)
       

includedIn :: [Interval] -> [Interval] -> Bool
includedIn i1 i2 = List.all (\iv1 -> List.any (\iv2 -> coveredBy iv1 iv2) i2) i1

merge :: Ord a => VV a -> VV a -> VV a
merge vv1 vv2 = unionWith (\x y -> compact $ mergeIntervals x y) vv1 vv2

mergeIntervals :: [Interval] -> [Interval] -> [Interval]
mergeIntervals [] i2 = i2
mergeIntervals i1 [] = i1
mergeIntervals ((from1,to1):i1) ((from2,to2):i2) 
  | coveredBy (from1,to1) (from2,to2) = mergeIntervals i1 ((from2,to2):i2)
  | coveredBy (from2,to2) (from1,to1) = mergeIntervals ((from1,to1):i1) i2
  | to1 < from2                       = (from1,to1) : mergeIntervals i1 ((from2,to2):i2)
  | to2 < from1                       = (from2,to2) : mergeIntervals ((from1,to1):i1) i2
  | otherwise  -- overlaps (from1,to1) (from2,to2)  
                                      = case i1 of
                                           [] -> mergeIntervals [(min from1 from2, max to1 to2)] i2
                                           (from1',to1'):i1' | max to1 to2 < from1' -> mergeIntervals ((min from1 from2, max to1 to2):i1) i2
                                                             | otherwise            -> mergeIntervals ((min from1 from2, to1'):i1) i2
 

coveredBy (from1,to1) (from2,to2) =  from2 <= from1 && to1 <= to2  
overlaps (from1,to1) (from2,to2) =  from2 <= from1 && from1 <= to2 || from1 <= from2 && from2 <= to1

valid :: Ord a => VV a -> Bool
valid vv = 
  let validInterval (from,to)   = from <= to && 0 <= from && to > 0
      validNext (_,to) (from,_) = to + 1 < from 
      validSequence x = case x of
        []  -> True
        [_] -> True
        a:b:xs -> validNext a b && validSequence (b:xs)
  in  List.all (\x -> any validInterval x  && validSequence x) (elems vv)



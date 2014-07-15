
module Main where


import System.Environment
import System.Exit

import Run


main :: IO ()
main = getArgs >>= run >>= exitWith

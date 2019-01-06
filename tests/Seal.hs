import Test.Hspec

import qualified PersistSpec
-- import Pact.PersistPactDb.Regression
-- import Control.Monad
-- import qualified Pact.Persist.MPTree as M
-- import qualified Pact.Persist.Pure as P
-- import qualified Pact.Persist.SQLite as T




main :: IO ()
main = hspec spec
-- main = do
--     putStrLn "MPTree Test"
--     M._test
    -- putStrLn ""
    -- putStrLn "Pure Test"
    -- P._test
    -- putStrLn ""
    -- putStrLn "SQLite Test"
    -- T._test
    -- PersistSpec.spec
    -- return ()



spec :: Spec
spec = do
--     it "regress MPtree" (void regressMPtree)

  describe "Test MP"     PersistSpec.spec
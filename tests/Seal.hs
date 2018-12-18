import Test.Hspec

-- import qualified PersistSpec
import Pact.PersistPactDb.Regression
import Control.Monad



main :: IO ()
main = hspec spec

spec :: Spec
spec = do
    it "regress MPtree" (void regressMPtree)
--   describe "Test MP"     PersistSpec.spec
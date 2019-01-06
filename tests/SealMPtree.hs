{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

-- module SealMPtree where

import Pact.Server.SealService
import Pact.Types.Command

import Seal.DB.MerklePatricia
import Pos.DB.Rocks.Functions  
import Data.ByteString (ByteString)
import Pact.Types.Hash
-- import Control.Concurrent (threadDelay)




main :: IO ()
main = do
    mpdb <- initMPdb
    putStrLn $ "begin CommandExecHandler"
    CommandExecHandler {..} <- initSealPactService True mpdb
    putStrLn $ "begin _applyCmd"
    result <- _applyCmd testCommand
    -- threadDelay 5000000
    putStrLn $ "end _applyCmd" ++ show result
    return ()



testCommand :: Command ByteString
testCommand = Command {
    _cmdPayload = testCom
   ,_cmdSigs    = []
   ,_cmdHash    = hash testCom
}

testCom :: ByteString
testCom = 
    "{\"address\":null,\"payload\":{\"exec\":{\"data\":{\"alice-keyset\":[\"7d0c9ba189927df85c8c54f8b5c8acd76c1d27e923abbf25a957afdf25550804\"],\"bob-keyset\":[\"ac69d9856821f11b8e6ca5cdd84a98ec3086493fd6407e74ea9038407ec9eba9\"]},\"code\":\"(use 'accounts)\\n(use 'system)\\n(create-account \\\"Alice\\\" (read-keyset \\\"alice-keyset\\\") \\\"USD\\\" (get-system-time))\\n(create-account \\\"Bob\\\" (read-keyset \\\"bob-keyset\\\") \\\"USD\\\" (get-system-time))\\n(fund-account \\\"Alice\\\" 100.0 (get-system-time))\\n(read-all)\"}},\"nonce\":\"\\\"step03\\\"\"}"


initMPdb :: IO MPDB
initMPdb = do
    db <- openRocksDB "/tmp/contract"
    let rdb' = MPDB {rdb=db,stateRoot=emptyTriePtr}
    initializeBlank rdb'
    return rdb'

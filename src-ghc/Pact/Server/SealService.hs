{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}


module Pact.Server.SealService
 (
    initSealPactService
   ,initSealContract
   ,execSealPactCommand
   ,CommandExecHandler(..)
 ) where


import Control.Concurrent
-- import Data.Maybe (fromMaybe)

import Pact.Types.Command
import Pact.Types.Logger
import Pact.Interpreter
import Pact.Server.Server(initFastLogger)
import Pact.Server.PactService(applyCmd)
import Pact.Types.Runtime(EntityName(..))
import Pact.Types.Server (CommandState(..))
import Pact.Gas
import Pact.Types.Runtime hiding (PublicKey)

-- import Control.Exception
import Data.Default
import Seal.DB.MerklePatricia
import Data.ByteString (ByteString)
import qualified Data.Map.Strict as M
-- import qualified Data.ByteString.Char8 as B
import Pact.Types.Hash
import Control.Concurrent (threadDelay)
import Universum ((<>))



data CommandExecHandler = CommandExecHandler {
    _applyCmd :: Command ByteString -> IO CommandResult
--   , _persist :: s -> m StateRoot
}


--mptreedb的路径 --初始化数据库 -- 初始化内置合约
--filepath是否需要读配置文件?
--启动的时候初始化？
initSealPactService :: Bool -> MPDB -> IO CommandExecHandler
initSealPactService verbose mpdb = do
    debugFn <- if verbose then initFastLogger else return (return . const ())
    let loggers = initLoggers debugFn doLog def
        logger  = newLogger loggers "SealPactService"
        klog s  = logLog logger "INIT" s
        -- gasLimit = fromMaybe 0 Nothing
        -- gasRate = fromMaybe 0 Nothing
        gasEnv = (GasEnv 0 0.0 (constGasModel 0))
    p <- mkMPtreeEnv loggers mpdb
    klog "Creating Pact Schema"
    initSchema p
    threadDelay 5000000 --测试用
    -- 暴露出外部接口
    cmdVar <- newMVar (CommandState initRefStore M.empty)
    let handler = CommandExecHandler {
        _applyCmd = \cmd -> applyCmd logger (Just (EntityName "entity")) p cmdVar gasEnv (Transactional 1) cmd (verifyCommand cmd)
      }
    -- 初始化内置合约
    result <- execSealPactCommand handler initSealContract
    klog $ "inside:" ++ show result
    -- _ <- _applyCmd initSealContract
    return handler
    


-- 初始化内置合约
initSealContract ::  (Command ByteString)
initSealContract =
     Command {
            _cmdPayload = contract
           ,_cmdSigs    = ["7d0c9ba189927df85c8c54f8b5c8acd76c1d27e923abbf25a957afdf25550804"]
           ,_cmdHash    = hash contract }

contract :: ByteString
contract = "{\"address\":null,\"payload\":{\"exec\":{\"data\":{\"admin-keyset\":[\"7d0c9ba189927df85c8c54f8b5c8acd76c1d27e923abbf25a957afdf25550804\"]},\"code\":\"" 
    <> sealContract <> "\"}},\"nonce\":\"\\\"step03\\\"\"}"



sealContract :: ByteString
sealContract = 
     "(define-keyset 'admin-keyset (read-keyset \\\"admin-keyset\\\"))\\n(defcontract helloWorld 'admin-keyset\\n\\\"A smart contract to greet the world.\\\"\\n(defn hello [name]\\n\\\"Do the hello-world dance\\\"\\n(format \\\"Hello {}!\\\" [name])))"

-- 调用合约 如何拿到环境变量db,内置合约?
execSealPactCommand :: CommandExecHandler -> Command ByteString -> IO CommandResult
execSealPactCommand CommandExecHandler {..} cmd = _applyCmd cmd
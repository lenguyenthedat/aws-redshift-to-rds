
module Options where


import Data.Char
import System.IO
import System.Environment
import System.Exit

import Options.Applicative


withOptions :: [String] -> (CopyTableOptions -> IO ExitCode) -> IO ExitCode
withOptions args action = case execParserPure parserPrefs parser args of
    Success a -> action a
    Failure (ParserFailure f) -> do
      (errorMessage, exitCode) <- f <$> getProgName
      hPutStrLn stderr errorMessage
      return exitCode
    CompletionInvoked _ -> do
      hPutStrLn stderr "completion not supported"
      return $ ExitFailure 1
  where
    parserPrefs = ParserPrefs "" False False True 80

data DBType
  = Redshift
  | Rds
    deriving (Enum, Bounded, Show)

data CopyTableOptions
  = FullOptions {
    fromType :: DBType,
    fromConnectionString :: String,
    fromTable :: Table,

    toType :: DBType,
    toConnectionString :: String,
    toTable :: Table
  }
  -- not yet implemented
  | ConfigFileOptions {
    foo :: String
  }
    deriving Show

data Table = Table {
    table_schema :: String,
    table_name :: String
  }

instance Show Table where
    show (Table schema table) = schema <> "." <> table


parser :: ParserInfo CopyTableOptions
parser =
    info (helper <*> bothOptions)
      (fullDesc
       <> progDesc "Copy a table from one database to another"
       <> header "copytables - a tool for copying tables between different databases")
  where
    bothOptions :: Parser CopyTableOptions
    bothOptions = fullOptions <|> configFileOptions

    configFileOptions :: Parser CopyTableOptions
    configFileOptions = empty -- ConfigFileOptions <$>
      -- strOption (long "foo")

    fullOptions :: Parser CopyTableOptions
    fullOptions = FullOptions <$>
      typeOption "from-type" <*>
      connectionStringOption "from-db" <*>
      tableOption "from-table" <*>

      typeOption "to-type" <*>
      connectionStringOption "to-db" <*>
      tableOption "to-table"

    typeOption :: String -> Parser DBType
    typeOption name = readDBType <$> strOption (
      long name <>
      metavar "DB_TYPE" <>
      help "type of the database. One of 'redshift', 'rds'.")
    readDBType :: String -> DBType
    readDBType "redshift" = Redshift
    readDBType "rds" = Rds
    readDBType t = error ("unsupported database type '" ++ t ++
      "'. Must be one of " ++
      show (map (map toLower . show)
        [minBound :: DBType .. maxBound]) ++ ".")

    connectionStringOption :: String -> Parser String
    connectionStringOption name = strOption (
      long name <>
      metavar "CONNECTION_STRING" <>
      help "connection string for accessing the database")

    tableOption :: String -> Parser Table
    tableOption name = readTable <$> strOption (
      long name <>
      metavar "TABLE" <>
      help "qualified table name ('schema.table')")
    readTable :: String -> Table
    readTable s = case span (/= '.') s of
      ("", name) -> error ("please specify table names as 'SCHEMA.NAME'. ('" ++ name ++ "')")
      (schema, '.' : name) -> Table schema name
      _ -> error ("invalid table format: " ++ s)

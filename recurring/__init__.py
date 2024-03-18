# ruff: noqa: F403, F405, E402, E501, E722
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.GRPCClient.CCD_Types import *
from ccdefundamentals.tooter import Tooter
from ccdefundamentals.mongodb import (
    MongoDB,
    Collections,
    CollectionsUtilities,
    MongoMotor,
)

from pymongo.collection import Collection
from env import *
from rich.console import Console
import urllib3
import aiohttp

from .mongo_tps_table import MongoTPSTable as _mongo_tps_table
from .get_dashboard_nodes import DashboardNodes as _dashboard_nodes
from .mongo_accounts_table import MongoAccountsTable as _mongo_accounts_table
from .get_exchange_rates import ExchangeRates as _exchange_rates
from .get_web23_metadata import Web23 as _web_23
from .get_metadata_if_null import MetaData as _metadata
from .get_main_tokens_page import TokensPage as _tokenspage
from .get_addresses_by_contract import AddressesByContract as _addresses
from .get_tokens_by_address import TokensByAddress as _tokens
from .update_memos_to_hashes import Memos as _memos
from .update_impacted_address_top import ImpactedAddresses as _impacted_addresses

urllib3.disable_warnings()
console = Console()


class Recurring(
    _mongo_tps_table,
    _mongo_accounts_table,
    _dashboard_nodes,
    _exchange_rates,
    _web_23,
    _metadata,
    _tokenspage,
    _addresses,
    _tokens,
    _memos,
    _impacted_addresses,
):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
    ):
        self.grpcclient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.utilities: dict[CollectionsUtilities, Collection] = self.mongodb.utilities

        self.motor_mainnet: dict[Collections, Collection] = self.motormongo.mainnet
        self.motor_testnet: dict[Collections, Collection] = self.motormongo.testnet
        self.session = aiohttp.ClientSession()
        self.quick_session = aiohttp.ClientSession(read_timeout=1)
        self.slow_session = aiohttp.ClientSession(read_timeout=10)
        coin_api_headers = {
            "X-CoinAPI-Key": COIN_API_KEY,
        }
        self.coin_api_session = aiohttp.ClientSession(headers=coin_api_headers)

    def exit(self):
        self.session.close()
        self.coin_api_session.close()
        self.quick_session.close()
        self.slow_session.close()

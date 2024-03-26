from __future__ import annotations

from recurring import Recurring
import atexit

from ccdexplorer_fundamentals.GRPCClient import GRPCClient
import asyncio
from ccdexplorer_fundamentals.tooter import Tooter
from ccdexplorer_fundamentals.mongodb import (
    MongoDB,
    MongoMotor,
)

grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)


def main():
    recurring = Recurring(grpcclient, tooter, mongodb, motormongo)
    atexit.register(recurring.exit)
    loop = asyncio.get_event_loop()
    # fmt: off
    loop.create_task(recurring.perform_statistics_mongo_tps_table())
    loop.create_task(recurring.perform_statistics_mongo_accounts_table())
    loop.create_task(recurring.update_nodes_from_dashboard())
    loop.create_task(recurring.update_exchange_rates_for_tokens())
    # loop.create_task(recurring.update_exchange_rates_historical_for_tokens())
    loop.create_task(recurring.web23_domain_name_metadata())
    loop.create_task(recurring.read_token_metadata_if_not_present())
    loop.create_task(recurring.get_tokens_page())
    loop.create_task(recurring.pre_addresses_by_contract_count())
    loop.create_task(recurring.update_memos_to_hashes())
    loop.create_task(recurring.update_impacted_addresses_all_top_list())
    # fmt: on
    loop.run_forever()


if __name__ == "__main__":
    main()

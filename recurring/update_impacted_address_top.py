from rich.console import Console
from .utils import Utils, RecurringType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import Collections
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
import datetime as dt
from ccdexplorer_fundamentals.enums import NET
import asyncio

console = Console()


class ImpactedAddresses(Utils):

    async def update_impacted_addresses_all_top_list(self):
        while True:
            self.mainnet: dict[Collections, Collection]
            recurring_type = RecurringType.impacted_addresses_top

            for net in NET:
                s = self.start_time_and_log(recurring_type, net)

                db: dict[Collections, Collection] = (
                    self.mainnet if net.value == "mainnet" else self.testnet
                )
                motordb: dict[Collections, Collection] = (
                    self.motor_mainnet if net.value == "mainnet" else self.motor_testnet
                )
                try:
                    # this is the block we last processed making the top list
                    bot_last_processed_block_for_top_list = db[
                        Collections.helpers
                    ].find_one(
                        {
                            "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list"
                        }
                    )
                    bot_last_processed_block_for_top_list_height = (
                        bot_last_processed_block_for_top_list["height"]
                    )

                    # this is the last finalized block in the collection of blocks
                    heartbeat_last_processed_block = db[Collections.helpers].find_one(
                        {"_id": "heartbeat_last_processed_block"}
                    )
                    heartbeat_last_processed_block_height = (
                        heartbeat_last_processed_block["height"]
                    )

                    pipeline = [
                        {
                            "$match": {
                                "block_height": {
                                    "$gt": bot_last_processed_block_for_top_list_height
                                }
                            }
                        },
                        {
                            "$match": {
                                "block_height": {
                                    "$lte": heartbeat_last_processed_block_height
                                }
                            }
                        },
                        {  # this filters out account rewards, as they are special events
                            "$match": {"tx_hash": {"$exists": True}},
                        },
                        {
                            "$group": {
                                "_id": "$impacted_address_canonical",
                                "count": {"$sum": 1},
                            }
                        },
                        {"$sort": {"count": -1}},
                    ]
                    result = (
                        await motordb[Collections.impacted_addresses]
                        .aggregate(pipeline)
                        .to_list(100)
                    )

                    local_queue = []

                    # get previously stored results
                    previous_result = db[
                        Collections.impacted_addresses_all_top_list
                    ].find({})
                    previous_accounts_dict = {
                        x["_id"]: x["count"] for x in previous_result
                    }
                    new_results = {}
                    for r in result:
                        if previous_accounts_dict.get(r["_id"]):
                            new_results[r["_id"]] = r[
                                "count"
                            ] + previous_accounts_dict.get(r["_id"])

                        else:
                            new_results[r["_id"]] = r["count"]

                    # now the new counts are added, but we need to add back the previous counts
                    # to come to a new top 100
                    for p, value in previous_accounts_dict.items():
                        # this account did not appear in the new list
                        if p not in new_results:
                            new_results[p] = value

                    # now new_results possibly has more than 100 entries, so sort and cut.
                    sorted_dict = sorted(
                        new_results.items(), key=lambda x: x[1], reverse=True
                    )

                    for index, (k, v) in enumerate(sorted_dict):
                        if index < 100:
                            local_queue.append(
                                ReplaceOne({"_id": k}, {"count": v}, upsert=True)
                            )

                    # _ = self.db[Collections.impacted_addresses_all_top_list].delete_many({})
                    if len(local_queue) > 0:
                        _ = db[Collections.impacted_addresses_all_top_list].delete_many(
                            {}
                        )
                        _ = db[Collections.impacted_addresses_all_top_list].bulk_write(
                            local_queue
                        )

                        query = {
                            "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list"
                        }
                        db[Collections.helpers].replace_one(
                            query,
                            {
                                "_id": "heartbeat_last_block_processed_impacted_addresses_all_top_list",
                                "height": heartbeat_last_processed_block_height,
                            },
                            upsert=True,
                        )
                        query = {
                            "_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list"
                        }
                        db[Collections.helpers].replace_one(
                            query,
                            {
                                "_id": "heartbeat_last_timestamp_impacted_addresses_all_top_list",
                                "timestamp": dt.datetime.now().astimezone(
                                    dt.timezone.utc
                                ),
                            },
                            upsert=True,
                        )
                    self.end_and_log(recurring_type, s, net)
                except Exception as e:
                    self.tooter: Tooter
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Recurring: Failed to update impacted addresses top. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
            await asyncio.sleep(60)

from rich.console import Console
from .utils import Utils, RecurringType
from ccdexplorer_fundamentals.mongodb import Collections
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
from itertools import chain
from ccdexplorer_fundamentals.cis import MongoTypeTokenAddress, MongoTypeTokensTag
import asyncio

console = Console()


class MetaData(Utils):
    async def read_token_metadata_if_not_present(self):
        """
        This method looks into the token_addresses collection specifically for
        tokenIDs from contract 9377 (the Web23 CCD contract).
        As they have not implemented the metadata log event, we need to perform
        this ourselves.
        As such, every time this runs, it retrieves all tokenIDs from this contract
        and loops through all tokenIDs that do not have metadata set (mostly new, could
        also be that in a previous run, there was a http issue).
        For every tokenID withou metadata, there is a call to the wallet-proxy to get
        the metadataURL, which is then stored in the collection. Finally, we read the
        metadataURL to determine the actual domainname and store this is a separate
        collection.
        """
        while True:
            self.motor_mainnet: dict[Collections, Collection]
            recurring_type = RecurringType.read_metadata
            s = self.start_time_and_log(recurring_type)
            try:
                token_tags = self.mainnet[Collections.tokens_tags].find({})

                recognized_contracts = [
                    MongoTypeTokensTag(**x).contracts for x in token_tags
                ]

                pipeline = [
                    {"$match": {"token_metadata": {"$not": {"$ne": None}}}},
                    {
                        "$match": {
                            "contract": {
                                "$in": list(chain.from_iterable(recognized_contracts))
                            }
                        }
                    },
                ]

                current_result = (
                    await self.motor_mainnet[Collections.tokens_token_addresses_v2]
                    .aggregate(pipeline)
                    .to_list(1000)
                )

                if len(current_result) > 0:
                    current_content = [
                        MongoTypeTokenAddress(**x) for x in current_result
                    ]
                else:
                    current_content = []

                for dom in current_content:
                    if dom.token_metadata:
                        continue

                    await self.read_and_store_metadata(dom, self.mainnet)

                self.end_and_log(recurring_type, s)
            except Exception as e:
                self.tooter: Tooter
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Recurring: Failed to read metadata. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )
            await asyncio.sleep(60)

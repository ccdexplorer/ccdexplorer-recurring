from rich.console import Console
from .utils import Utils, RecurringType
from ccdexplorer_fundamentals.mongodb import Collections
from ccdexplorer_fundamentals.GRPCClient.CCD_Types import CCD_ContractAddress
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType
from ccdexplorer_fundamentals.enums import NET
import aiohttp
from ccdexplorer_fundamentals.cis import (
    MongoTypeTokenAddress,
    MongoTypeTokensTag,
)
import asyncio

console = Console()


class Web23(Utils):
    def save_token_address(
        self,
        token_address_as_class: MongoTypeTokenAddress,
        db: dict[Collections, Collection],
    ):
        dom_dict = token_address_as_class.model_dump(exclude_none=True)
        if "id" in dom_dict:
            del dom_dict["id"]
        db[Collections.tokens_token_addresses_v2].replace_one(
            {"_id": token_address_as_class.id},
            replacement=dom_dict,
            upsert=True,
        )

    async def web23_domain_name_metadata(self):
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
            for net in NET:
                db: dict[Collections, Collection] = (
                    self.mainnet if net.value == "mainnet" else self.testnet
                )
                motor_db: dict[Collections, Collection] = (
                    self.motor_mainnet if net.value == "mainnet" else self.motor_testnet
                )
                recurring_type = RecurringType.web23_metadata
                s = self.start_time_and_log(recurring_type, net)

                try:
                    ccd_token_tags = db[Collections.tokens_tags].find_one(
                        {"_id": ".ccd"}
                    )

                    if ccd_token_tags:
                        contracts_in_ccd_token_tag = MongoTypeTokensTag(
                            **ccd_token_tags
                        ).contracts

                        pipeline = [
                            {"$match": {"token_metadata": {"$exists": False}}},
                            {
                                "$match": {
                                    "contract": {"$in": contracts_in_ccd_token_tag}
                                }
                            },
                        ]

                        current_result = (
                            await motor_db[Collections.tokens_token_addresses_v2]
                            .aggregate(pipeline)
                            .to_list(100_000)
                        )
                        if len(current_result) > 0:
                            current_content = [
                                MongoTypeTokenAddress(**x) for x in current_result
                            ]
                        else:
                            current_content = []
                    else:
                        current_content = []

                    timeout = 1  # sec
                    session = aiohttp.ClientSession(read_timeout=timeout)
                    for dom in current_content:
                        if dom.token_metadata:
                            continue
                        contract_index = CCD_ContractAddress.from_str(
                            dom.contract
                        ).index
                        if net.value == "testnet":
                            url_to_fetch_metadata = f"https://wallet-proxy.testnet.concordium.com/v0/CIS2TokenMetadata/{contract_index}/0?tokenId={dom.token_id}"
                        else:
                            url_to_fetch_metadata = f"https://wallet-proxy.mainnet.concordium.software/v0/CIS2TokenMetadata/{contract_index}/0?tokenId={dom.token_id}"

                        try:
                            async with session.get(url_to_fetch_metadata) as resp:
                                t = await resp.json()

                                dom.metadata_url = None
                                if resp.status == 200:
                                    try:
                                        token_metadata = t
                                        if "metadata" in token_metadata:
                                            if (
                                                "metadataURL"
                                                in token_metadata["metadata"][0]
                                            ):
                                                dom.metadata_url = token_metadata[
                                                    "metadata"
                                                ][0]["metadataURL"]
                                                await self.read_and_store_metadata(
                                                    dom, db
                                                )
                                    except Exception as e:
                                        console.log(e)
                                        dom.metadata_url = None

                        except Exception as e:
                            console.log(e)
                    await session.close()
                    self.end_and_log(recurring_type, s, net)

                except Exception as e:
                    self.tooter: Tooter
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Recurring: Failed to web23 metadata for {net.value}. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )
            await asyncio.sleep(60)

from rich.console import Console
from .utils import Utils, RecurringType
from pymongo import ReplaceOne
from ccdexplorer_fundamentals.mongodb import Collections
from ccdexplorer_fundamentals.node import ConcordiumNodeFromDashboard
from pymongo.collection import Collection
from ccdexplorer_fundamentals.tooter import Tooter, TooterChannel, TooterType

import datetime as dt
import aiohttp
from ccdexplorer_fundamentals.enums import NET
import asyncio

console = Console()


class DashboardNodes(Utils):
    async def update_nodes_from_dashboard(self, net: str = "mainnet"):
        while True:
            try:
                for net in NET:
                    db: dict[Collections, Collection] = (
                        self.mainnet if net.value == "mainnet" else self.testnet
                    )
                    recurring_type = RecurringType.dashboard_nodes
                    s = self.start_time_and_log(recurring_type, net)
                    async with aiohttp.ClientSession() as session:
                        if net.value == "testnet":
                            url = (
                                "https://dashboard.testnet.concordium.com/nodesSummary"
                            )
                        else:
                            url = "https://dashboard.mainnet.concordium.software/nodesSummary"
                        async with session.get(url) as resp:
                            t = await resp.json()

                            queue = []

                            for raw_node in t:
                                node = ConcordiumNodeFromDashboard(**raw_node)
                                d = node.model_dump()
                                d["_id"] = node.nodeId

                                for k, v in d.items():
                                    if isinstance(v, int):
                                        d[k] = str(v)

                                queue.append(
                                    ReplaceOne({"_id": node.nodeId}, d, upsert=True)
                                )

                            _ = db[Collections.dashboard_nodes].delete_many({})
                            _ = db[Collections.dashboard_nodes].bulk_write(queue)
                            #
                            #
                            # update nodes status retrieval
                            query = {"_id": "heartbeat_last_timestamp_dashboard_nodes"}
                            db[Collections.helpers].replace_one(
                                query,
                                {
                                    "_id": "heartbeat_last_timestamp_dashboard_nodes",
                                    "timestamp": dt.datetime.now().astimezone(
                                        tz=dt.timezone.utc
                                    ),
                                },
                                upsert=True,
                            )
                            self.end_and_log(recurring_type, s, net)
            except Exception as e:
                self.tooter: Tooter
                self.tooter.send(
                    channel=TooterChannel.NOTIFIER,
                    message=f"Recurring: Failed to get dashboard nodes for {net.value}. Error: {e}",
                    notifier_type=TooterType.REQUESTS_ERROR,
                )
            await asyncio.sleep(5 * 60)

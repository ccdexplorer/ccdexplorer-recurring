[![DockerHub Image](https://github.com/ccdexplorer/ccdexplorer-recurring/actions/workflows/docker-image.yml/badge.svg?branch=main)](https://github.com/ccdexplorer/ccdexplorer-recurring/actions/workflows/docker-image.yml)
# CCDExplorer-Recurring

This repo executes recurring tasks and pre-fetches expensive queries.

Methods:
1. recurring.perform_statistics_mongo_tps_table
2. recurring.perform_statistics_mongo_accounts_table
3. recurring.update_nodes_from_dashboard
4. recurring.update_exchange_rates_for_tokens
5. recurring.update_exchange_rates_historical_for_tokens
6. recurring.web23_domain_name_metadata
7. recurring.read_token_metadata_if_not_present
8. recurring.get_tokens_page
9. recurring.pre_addresses_by_contract_count
10. recurring.update_memos_to_hashes
11. recurring.update_impacted_addresses_all_top_list
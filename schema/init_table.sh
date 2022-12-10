# BigQueryのテーブル作成シェル
# テーブル作成
inv create-table -d bid_optimisation_ml -t bidding_unit_info -p data_date
inv create-table -d bid_optimisation_ml -t bidding_ad_performance -p data_date
inv create-table -d bid_optimisation_ml -t ml_result_unit -p date
inv create-table -d bid_optimisation_ml -t ml_result_campaign -p date
inv create-table -d bid_optimisation_ml -t ml_result_ad -p date

import pandas as pd
import numpy as np
from common_module.logger_util import get_custom_logger


logger = get_custom_logger()


class OutputFormat:
    RESULT_TYPE_OPTIMIZED = 1
    RESULT_TYPE_NO_RESULT = 2

    def __init__(self, advertising_account_id, portfolio_id, all_data_df):
        self.advertising_account_id = advertising_account_id
        self.portfolio_id = portfolio_id

        if portfolio_id is None:
            self.unit_df = all_data_df[
                (all_data_df["advertising_account_id"] == advertising_account_id)
                & (all_data_df["portfolio_id"].isnull())
            ].reset_index()
        else:
            self.unit_df = all_data_df[
                (all_data_df["advertising_account_id"] == advertising_account_id)
                & (all_data_df["portfolio_id"] == portfolio_id)
            ].reset_index()

        self.unit_df["daily_budget"] = self._cast_optional_int(
            self.unit_df, "daily_budget"
        )
        self.unit_df["bidding_price"] = self._cast_float(self.unit_df, "bidding_price")
        self.unit_df["last_bidding_price"] = self._cast_float(
            self.unit_df, "last_bidding_price"
        )

        # treat null as true
        self.unit_df["is_enabled_daily_budget_auto_adjustment"].fillna(
            True, inplace=True
        )
        # キャンペーンに対して（＝ad_idがnull）はfillnaしない
        self.unit_df["is_enabled_bidding_auto_adjustment"] = self.unit_df[
            "is_enabled_bidding_auto_adjustment"
        ] | (
            self.unit_df["is_enabled_bidding_auto_adjustment"].isnull()
            & (~self.unit_df["ad_id"].isnull())
        )
        self.unit_df["portfolio_id"] = self.unit_df["portfolio_id"].astype(
            pd.Int64Dtype()
        )

        self.is_ml_daily_budget_enabled = self._is_ml_enabled("daily_budget", False)
        self.is_ml_bidding_price_enabled = self._is_ml_enabled("bidding_price", True)

        self.output_df = None

    def _cast_optional_int(self, df, field_name):
        return df[field_name].astype(pd.Int64Dtype())

    def _cast_float(self, df, field_name):
        return df[field_name].astype("float64")

    def _get_only_cap_mask(self, df):
        """日予算アロケのみ有効な行のマスクを作成する"""
        col = "is_enabled_bidding_auto_adjustment"
        has_no_ads = ~df.groupby("campaign_id")[col].any()
        col = "is_enabled_daily_budget_auto_adjustment"
        is_enabled_cap = df.groupby("campaign_id")[col].any()
        only_cap_cps = (has_no_ads & is_enabled_cap).rename("only_cap").to_frame()
        df = pd.merge(df, only_cap_cps, on="campaign_id", how="left")

        return df["only_cap"]

    def _ad_count(self, df):
        """キャンペーンごとの出力対象の広告の数をカウントする"""
        ad_count_df = (
            df.loc[~pd.isnull(df["ad_id"]), :]
            .groupby(["campaign_id"])["ad_id"]
            .count()
            .rename("ad_count")
            .reset_index()
        )

        df = pd.merge(df, ad_count_df, on=["campaign_id"], how="left")

        df["ad_count"] = df["ad_count"].fillna(0)

        return df

    def _is_ml_enabled(self, field_name, ignore_only_cap_row):
        if ignore_only_cap_row:
            mask = ~self._get_only_cap_mask(self.unit_df)
        else:
            mask = [True] * len(self.unit_df)

        is_null = self.unit_df.loc[mask, field_name].isnull()
        if is_null.any():
            logger.error(
                f"ML enabled but any {field_name} is missing."
                + f" advertising_account_id:{self.advertising_account_id}"
                + f" ,portfolio_id: {self.portfolio_id}"
            )
            for i in is_null[is_null].index:
                logger.info(f"This row has null\n{self.unit_df.loc[i]}")
            return False
        else:
            return True

    @classmethod
    def _get_unique_ids(cls, df, fields):
        return df[fields].groupby(fields).first().reset_index().dropna()

    def is_ml_enabled(self, df, fields):
        return self.is_ml_daily_budget_enabled and self.is_ml_bidding_price_enabled

    def _make_empty_campaign_df(self, campaign_id, data_df):
        criteria = f"campaign_id == {campaign_id}"

        campaigns_df = data_df.query(criteria, engine="python")

        campaign_head_df = campaigns_df.iloc[0]

        daily_budget = int(campaign_head_df.daily_budget)

        return pd.DataFrame(
            {
                "advertising_account_id": [self.advertising_account_id],
                "portfolio_id": [self.portfolio_id],
                "campaign_id": [campaign_id],
                "daily_budget": [daily_budget],
                "last_daily_budget": [int(campaign_head_df.last_daily_budget)],
            }
        )

    def _make_empty_unit_df(self):
        return pd.DataFrame(
            {
                "advertising_account_id": [self.advertising_account_id],
                "portfolio_id": [self.portfolio_id],
                "result_type": [self.RESULT_TYPE_NO_RESULT],
            }
        )

    def _filter_ad(self, df):
        """広告の出力の条件"""
        # (入札額がnot null かつ 入札額変更あり) or ターゲット停止
        mask = (
            ~pd.isnull(df["bidding_price"])
            & (df["bidding_price"] != df["last_bidding_price"])
        ) | df["is_paused"]

        filtered_df = df.loc[mask, :]
        droped = df.loc[~mask, :]
        droped_campaigns = set(droped["campaign_id"].unique()) - set(
            filtered_df["campaign_id"].unique()
        )

        campaign_dfs = []
        if droped_campaigns:
            for campaign_id in list(droped_campaigns):
                campaign_dfs.append(self._make_empty_campaign_df(campaign_id, droped))
            filtered_df = pd.concat([filtered_df, pd.concat(campaign_dfs)])

        return filtered_df

    def _filter_campaign(self, df):
        """出力対象の広告が０件以上　or daily_budgetがnot nullのキャンペーンが出力対象"""
        mask_ad_count = df["ad_count"] > 0

        mask = df["daily_budget"] == df["last_daily_budget"]
        df.loc[mask, "daily_budget"] = None

        mask_budget = ~pd.isnull(df["daily_budget"])

        df = df.loc[mask_ad_count | mask_budget, :]

        return df

    def _filter_df(self):
        df = self.unit_df.copy()

        df = self._filter_ad(df)

        df = self._ad_count(df)

        df = self._filter_campaign(df)

        # result type
        if len(df[~pd.isnull(df["campaign_id"])]) > 0:
            df["result_type"] = self.RESULT_TYPE_OPTIMIZED
        else:
            return self._make_empty_unit_df()

        return df

    def get_output_df(self):
        return self.output_df if len(self.output_df) > 0 else None

    def get_formatted(self):
        if not self.is_ml_daily_budget_enabled or not self.is_ml_bidding_price_enabled:
            return None

        self.output_df = self._filter_df().reset_index()

        if "campaign_id" not in self.output_df.columns:
            return {
                "result_type": self.output_df.loc[0, "result_type"],
                "is_ml_enabled": True,
            }

        campaign_ids_df = self._get_unique_ids(self.output_df, ["campaign_id"])

        campaigns = []
        for ids_row in campaign_ids_df.itertuples():
            campaign = self._campaign(ids_row, self.output_df)
            if campaign is not None:
                campaigns.append(campaign)

        ret = {
            "result_type": self.output_df.loc[0, "result_type"],
            "is_ml_enabled": True,
        }

        if len(campaigns) > 0:
            ret["unit"] = {"campaigns": campaigns}

        return ret

    def _campaign(self, ids_row, data_df):
        campaign_id = int(ids_row.campaign_id)
        criteria = f"campaign_id == {campaign_id}"

        campaigns_df = data_df.query(criteria, engine="python")

        ad_ids_df = self._get_unique_ids(campaigns_df, ["ad_type", "ad_id"])

        campaign_head_df = campaigns_df.iloc[0]

        daily_budget = (
            None
            if pd.isnull(campaign_head_df.daily_budget)
            else int(campaign_head_df.daily_budget)
        )

        ads = []
        for ids_row in ad_ids_df.itertuples():
            ad = self._ad(ids_row, campaigns_df)
            if ad is not None:
                ads.append(ad)

        if len(ads) == 0 and daily_budget is None:
            return None
        else:
            ret = {
                "campaign_id": str(campaign_head_df.campaign_id),
                "daily_budget": daily_budget,
                "ads": ads if len(ads) > 0 else None,
            }

        return ret

    def _ad(self, ids_row, data_df):
        ad_type = ids_row.ad_type
        ad_id = ids_row.ad_id
        criteria = f"ad_type == '{ad_type}'" + f" and ad_id == {ad_id}"

        ad_df = data_df.query(criteria, engine="python")

        if len(ad_df) == 0:
            return None

        ad_head_df = ad_df.iloc[0]

        is_paused = bool(ad_head_df.is_paused)
        bidding_price = (
            None
            if np.isnan(ad_head_df.bidding_price)
            else int(ad_head_df.bidding_price)
        )

        return {
            "ad_id": f"{ad_head_df.ad_type}_{int(ad_head_df.ad_id)}",
            "is_paused": is_paused,
            "bidding_price": bidding_price,
        }

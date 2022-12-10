import datetime
from dataclasses import asdict, replace
import calendar
from typing import Optional


from .bq_tables import (
    AdvertisingAccount,
    Portfolio,
    Campaign,
    AdGroup,
    Keyword,
    KeywordQuery,
    ProductTargeting,
    Targeting,
    Daily,
    DailyKeyword,
    DailyKeywordQuery,
    DailyProductTargeting,
    DailyTargeting,
    DailyAdGroup,
    DailyCampaign,
    OptimizationAdSetting,
    OptimizationSetting,
    BiddingUnitInfo,
    BiddingAdPerformance,
    AdGroupHistory,
    BiddingHistory,
    KeywordHistory,
    ProductTargetingHistory,
    TargetingHistory,
    MLResultUnit,
    MLResultCampaign,
    MLResultAd,
)
from utils.testcase import TestCase, Performance


def get_first_date(dt):
    return dt.replace(day=1)


def get_last_date(dt):
    return dt.replace(day=calendar.monthrange(dt.year, dt.month)[1])


def get_remaining_days(dt):
    return (get_last_date(dt) - dt).days + 1


class Converter:
    daily_id: int = 0
    history_id: int = 0
    num_days_of_past_predictions: int = 14

    default_account_type: str = "seller"
    default_auto_stop_threshold_percent: int = 100
    default_country_code: str = "JP"
    default_currency_code: str = "JP"
    default_timezone: str = "Asia/Tokyo"
    default_bid_type: str = "sp"
    default_targeting_type: str = "manual"
    default_budget_type: str = "daily"
    default_bidding_strategy: Optional[str] = None
    default_ad_format: Optional[str] = None
    default_serving_status: Optional[str] = "CAMPAIGN_STATUS_ENABLED"
    default_status: str = "enabled"
    default_deviation_value_of_roas: float = 0
    default_tag_id: int = 111
    default_keyword_text: str = "keyword_text"
    default_match_type: str = "manual"
    default_nomination_type_id: int = 0
    default_expression_type: str = "manual"
    default_expression: str = "QueryHighRelMatches"
    default_query: str = "query"

    default_orders_new_to_brand: int = 0
    default_sales_new_to_brand: float = 0
    default_target_cost: float = 0
    default_range_type: str = "month"
    default_round_up_point: int = 1

    # def __init__(self, testcase: TestCase, performances: List[Performance]):
    def __init__(self, testcase: TestCase):
        self.today = testcase.processing_date

        self.testcase = testcase

        self.advertising_account = None
        self.portfolio = None
        self.campaign = None
        self.ad_group = None
        self.keyword = None
        self.keyword_query = None
        self.product_targeting = None
        self.targeting = None
        self.optimization_ad_setting = None
        self.optimization_setting = None
        self.bidding_unit_info = None

    def toAdvertisingAccount(self):
        out = AdvertisingAccount(
            id=self.testcase.advertising_account_id,
            amazon_id=self.testcase.id,
            amazon_entity_id=self.testcase.id,
            name=self.testcase.name,
            amazon_account_id=self.testcase.id,
            company_id=self.testcase.id,
            type=self.default_account_type,
            is_modified_name=False,
            auto_stop_threshold_percent=self.default_auto_stop_threshold_percent,
            is_enabled_auto_daily_budget_adjustment=self.testcase.unit.is_opt_enabled,
            is_enabled_auto_bid_adjustment=self.testcase.unit.is_opt_enabled,
            country_code=self.default_country_code,
            currency_code=self.default_currency_code,
            timezone=self.default_timezone,
            is_valid_for_advertising_api=True,
        )
        self.advertising_account = out
        return out

    def toPortfolio(self):
        if self.testcase.portfolio_id is not None:
            out = Portfolio(
                id=self.testcase.portfolio_id,
                name=self.testcase.name,
                advertising_account_id=self.testcase.advertising_account_id,
                auto_stop_threshold_percent=self.advertising_account.auto_stop_threshold_percent,
                is_enabled_auto_daily_budget_adjustment=(
                    self.advertising_account.is_enabled_auto_daily_budget_adjustment),
                is_enabled_auto_bid_adjustment=self.advertising_account.is_enabled_auto_bid_adjustment,
            )
            self.portfolio = out

        return self.portfolio

    def toCampaign(self):
        self.campaigns = [Campaign(
            id=campaign.id,
            amazon_id=self.advertising_account.amazon_id,
            name=self.testcase.name,
            advertising_account_id=self.testcase.advertising_account_id,
            portfolio_id=self.testcase.portfolio_id,
            status=self.default_status,
            type=self.default_bid_type,
            targeting_type=self.default_targeting_type,
            start_at=self.today,
            end_at=get_last_date(self.today),
            budget=campaign.current_daily_budget,
            budget_type=self.default_budget_type,
            bidding_strategy=self.default_bidding_strategy,
            tag_id=self.testcase.id,
            auto_campaign_id=self.testcase.id,
            ad_format=self.default_ad_format,
            serving_status=self.default_serving_status,
        ) for campaign in self.testcase.campaign.list]
        out = list()
        ranges = set()
        for offset in range(self.num_days_of_past_predictions + 1):
            today = self.today - datetime.timedelta(days=offset)
            ranges.add((get_first_date(today), get_last_date(today)))
        # for start_at, end_at in ranges:
        #     out = replace(self.campaign, start_at=start_at, end_at=end_at)
        out = self.campaigns
        return out

    def toAdGroup(self):
        out = [AdGroup(
            id=ad.campaign_id,
            amazon_id=self.advertising_account.amazon_id,
            campaign_id=ad.campaign_id,
            name=self.testcase.name,
            status=self.default_status,
            default_bid=ad.current_bidding_price,
            tag_id=self.default_tag_id,
        ) for ad in self.testcase.ad.list]
        self.ad_group = out
        return out

    def toKeyword(self):
        out = [Keyword(
            id=ad.id,
            amazon_id=self.advertising_account.amazon_id,
            campaign_id=ad.campaign_id,
            ad_group_id=ad.campaign_id,
            status=self.default_status,
            bidding_price=ad.current_bidding_price,
            deviation_value_of_roas=self.default_deviation_value_of_roas,
            tag_id=self.default_tag_id,
            amazon_ad_group_id=self.testcase.id,
            keyword_text=self.default_keyword_text,
            match_type=self.default_match_type,
            nomination_type_id=self.default_nomination_type_id,
        ) for ad in self.testcase.ad.list if ad.ad_type == "keyword"]
        self.keyword = out
        return out

    def toKeywordQuery(self):
        out = [KeywordQuery(
            id=ad.id,
            keyword_id=ad.id,
            query=self.default_query
        ) for ad in self.testcase.ad.list if ad.ad_type == "keyword"]
        self.keyword_query = out
        return out

    def toProductTargeting(self):
        out = [ProductTargeting(
            id=ad.id,
            amazon_id=self.advertising_account.amazon_id,
            campaign_id=ad.campaign_id,
            ad_group_id=ad.campaign_id,
            status=self.default_status,
            bidding_price=ad.current_bidding_price,
            deviation_value_of_roas=self.default_deviation_value_of_roas,
            tag_id=self.default_tag_id,
            expression_type=self.default_expression_type,
        ) for ad in self.testcase.ad.list if ad.ad_type == "product_targeting"]
        self.product_targeting = out
        return out

    def toTargeting(self):
        out = [Targeting(
            id=ad.id,
            amazon_id=self.advertising_account.amazon_id,
            campaign_id=ad.campaign_id,
            ad_group_id=ad.campaign_id,
            status=self.default_status,
            bidding_price=ad.current_bidding_price,
            deviation_value_of_roas=self.default_deviation_value_of_roas,
            tag_id=self.default_tag_id,
            amazon_ad_group_id=self.testcase.id,
            expression=self.default_expression,
        ) for ad in self.testcase.ad.list if ad.ad_type == "targeting"]
        self.targeting = out
        return out

    def _toDailyList(self, id, performance: Performance, history_days: int):
        out = list()
        for i in range(history_days):
            date = self.today - datetime.timedelta(days=i + 1)
            daily_id = int(f"{id}{i}")

            dummy_bidding_price = 100
            daily = Daily(
                date=date,
                id=daily_id,
                bidding_price=dummy_bidding_price,
                impressions=performance.impressions,
                clicks=performance.clicks,
                costs=performance.costs,
                conversions=performance.conversions,
                sales=performance.sales,
                orders_new_to_brand=self.default_orders_new_to_brand,
                sales_new_to_brand=self.default_sales_new_to_brand,
            )
            out.append(daily)
        return out

    @staticmethod
    def asdict(obj):
        d = asdict(obj)
        # remove created_at and udpated_at
        # because init=False
        if "created_at" in d:
            del d["created_at"]
        if "updated_at" in d:
            del d["updated_at"]
        return d

    def toDailyKeywordList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad.ad_type != "keyword":
                continue

            for o in self._toDailyList(ad.id, ad.performance, self.testcase.history_days):
                d = self.asdict(o)
                d["keyword_id"] = ad.id
                d["campaign_id"] = ad.campaign_id
                out.append(DailyKeyword(**d))
        return out

    def toDailyKeywordQueryList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad.ad_type != "keyword":
                continue

            for o in self._toDailyList(ad.id, ad.performance, self.testcase.history_days):
                d = self.asdict(o)
                d["keyword_query_id"] = ad.id
                out.append(DailyKeywordQuery(**d))
        return out

    def toDailyProductTargetingList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad.ad_type != "product_targeting":
                continue

            for o in self._toDailyList(ad.id, ad.performance, self.testcase.history_days):
                d = self.asdict(o)
                d["product_targeting_id"] = ad.id
                out.append(DailyProductTargeting(**d))
        return out

    def toDailyTargetingList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad.ad_type != "targeting":
                continue

            for o in self._toDailyList(ad.id, ad.performance, self.testcase.history_days):
                d = self.asdict(o)
                d["targeting_id"] = ad.id
                out.append(DailyTargeting(**d))
        return out

    def toDailyAdGroupList(self):
        out = list()
        for campaign in self.testcase.campaign.list:
            for o in self._toDailyList(campaign.id, campaign.performance, self.testcase.history_days):
                d = self.asdict(o)
                d["ad_group_id"] = campaign.id
                out.append(DailyAdGroup(**d))
        return out

    def toDailyCampaignList(self):
        out = list()
        for campaign in self.testcase.campaign.list:
            for o in self._toDailyList(campaign.id, campaign.performance, self.testcase.history_days):
                d = self.asdict(o)
                d["campaign_id"] = campaign.id
                out.append(DailyCampaign(**d))

        return out

    def toMLResultUnit(self):
        out = list()
        if self.testcase.num_past_days:
            for i in range(1, self.testcase.num_past_days + 1):
                date = self.today - datetime.timedelta(i)
                o = MLResultUnit(
                    advertising_account_id=self.testcase.advertising_account_id,
                    portfolio_id=self.testcase.portfolio_id,
                    date=date,
                    is_ml_enabled=True,
                    is_lastday_ml_applied=self.testcase.is_lastday_ml_applied,
                    target_cost=self.testcase.unit.yesterday_target_cost,
                    target_kpi=self.testcase.unit.purpose.upper()
                    if self.testcase.unit.purpose is not None else None,
                    **self.testcase.unit.yesterday_pid_result.dict(),
                )
                out.append(o)
        else:
            return None

        return out

    def toMLResultCampaign(self):
        out = list()
        if self.testcase.num_past_days:
            for campaign in self.testcase.campaign.list:
                for i in range(1, self.testcase.num_past_days + 1):
                    date = self.today - datetime.timedelta(i)
                    o = MLResultCampaign(
                        advertising_account_id=self.testcase.advertising_account_id,
                        portfolio_id=self.testcase.portfolio_id,
                        campaign_id=campaign.id,
                        date=date,
                        cap_daily_budget=campaign.current_daily_budget,
                        cap_daily_budget_weight=campaign.yesterday_daily_budget_weight,
                    )
                    out.append(o)
        else:
            return None

        return out

    def toMLResultAd(self):
        out = list()
        if self.testcase.num_past_days:
            for ad in self.testcase.ad.list:
                for i in range(1, self.testcase.num_past_days + 1):
                    date = self.today - datetime.timedelta(i)
                    o = MLResultAd(
                        advertising_account_id=self.testcase.advertising_account_id,
                        portfolio_id=self.testcase.portfolio_id,
                        campaign_id=ad.campaign_id,
                        ad_type=ad.ad_type,
                        ad_id=ad.id,
                        date=date,
                        bidding_price=ad.current_bidding_price,
                    )
                    out.append(o)
        else:
            return None

        return out

    def camel_ad_type(self, ad_type):
        if ad_type == "keyword":
            return "Keyword"
        elif ad_type == "product_targeting":
            return "ProductTargeting"
        elif ad_type == "targeting":
            return "Targeting"

    def toOptimizationAdSetting(self):
        out = [OptimizationAdSetting(
            id=ad.id,
            ad_id=ad.id,
            ad_type=self.camel_ad_type(ad.ad_type),
            is_bid_optimization=self.testcase.unit.is_opt_enabled,
        ) for ad in self.testcase.ad.list]
        self.optimization_ad_setting = out
        return out

    def purpose_int(self, purpose):
        return {
            "roas": 0,
            "cpa": 1,
            "cpc": 2,
        }[purpose]

    def toOptimizationSetting(self):
        out = OptimizationSetting(
            id=self.testcase.id,
            optimization_costs=self.testcase.unit.optimization_costs,
            is_auto_apply_bid_changes=True,
            is_auto_apply_daily_budget_changes=True,
            is_auto_apply_pause_keywords=True,
            optimization_priority_mode_type=self.testcase.unit.mode,
            optimization_purpose=self.purpose_int(self.testcase.unit.purpose),
            optimization_purpose_value=self.testcase.unit.target_kpi_value,
            optimization_completed_at=get_last_date(self.today),
            portfolio_id=self.testcase.portfolio_id,
        )
        self.optimization_setting = out
        return out

    def toBiddingUnitInfo(self):
        self.bidding_unit_info = BiddingUnitInfo(
            data_date=self.today,
            advertising_account_id=self.testcase.advertising_account_id,
            portfolio_id=self.testcase.portfolio_id,
            purpose=self.testcase.unit.purpose,
            target_cost=self.default_target_cost,
            type=self.default_range_type,
            mode=self.testcase.unit.mode,
            start=self.today - datetime.timedelta(days=100),
            end=self.today + datetime.timedelta(days=100),
            total_budget=self.testcase.unit.optimization_costs,
            target_value=self.testcase.unit.target_kpi_value,
            remaining_days=get_remaining_days(self.today),
            change_date=self.today,
            round_up_point=self.default_round_up_point,
        )
        out = list()
        for offset in range(self.num_days_of_past_predictions + 1):
            day = self.today - datetime.timedelta(days=offset)
            out.append(
                replace(
                    self.bidding_unit_info,
                    data_date=day,
                    end=get_last_date(day),
                    remaining_days=get_remaining_days(day),
                )
            )
        return out

    def toBiddingAdPerformanceList(self):
        out = list()
        for ad in self.testcase.ad.list:
            campaign = None
            for tmp in self.testcase.campaign.list:
                if tmp.id == ad.campaign_id:
                    campaign = tmp

            if campaign is None:
                raise ValueError("ad and campaign relation is not correct.")

            for o in self._toDailyList(ad.id, ad.performance, self.testcase.history_days):
                out.append(
                    BiddingAdPerformance(
                        data_date=self.today,
                        advertising_account_id=self.testcase.advertising_account_id,
                        portfolio_id=self.testcase.portfolio_id,
                        campaign_id=ad.campaign_id,
                        ad_type=ad.ad_type,
                        ad_id=ad.id,
                        daily_budget=campaign.current_daily_budget,
                        minimum_daily_budget=campaign.minimum_daily_budget,
                        maximum_daily_budget=campaign.maximum_daily_budget,
                        is_enabled_daily_budget_auto_adjustment=(
                            self.advertising_account.is_enabled_auto_daily_budget_adjustment),
                        is_enabled_bidding_auto_adjustment=self.advertising_account.is_enabled_auto_bid_adjustment,
                        minimum_bidding_price=ad.minimum_bidding_price,
                        maximum_bidding_price=ad.maximum_bidding_price,
                        date=o.date,
                        impressions=o.impressions,
                        clicks=o.clicks,
                        conversions=o.conversions,
                        sales=o.sales,
                        costs=o.costs,
                        bidding_price=ad.current_bidding_price,
                    )
                )

        return out

    def toAdGroupHistoryList(self):
        out = list()
        for ad in self.testcase.ad.list:
            self.history_id += 1
            date = self.today - datetime.timedelta(days=100)

            ad_group_history = AdGroupHistory(
                id=ad.id,
                reported_at=date,
                ad_group_id=ad.campaign_id,
                default_bid=ad.current_bidding_price,
                amazon_default_bid=ad.current_bidding_price
            )
            out.append(ad_group_history)
        return out

    def _toBiddingHistoryList(self, ad):
        out = list()
        date = self.today - datetime.timedelta(days=100)

        history = BiddingHistory(
            id=ad.id,
            reported_at=date,
            bidding_price=ad.current_bidding_price,
            amazon_bidding_price=ad.current_bidding_price,
        )

        out.append(history)

        return out

    def toKeywordHistoryList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad.ad_type != "keyword":
                continue

            for o in self._toBiddingHistoryList(ad):
                d = self.asdict(o)
                d["keyword_id"] = ad.id
                out.append(KeywordHistory(**d))
        return out

    def toProductTargetingHistoryList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad.ad_type != "product_targeting":
                continue

            for o in self._toBiddingHistoryList(ad):
                d = self.asdict(o)
                d["product_targeting_id"] = ad.id
                out.append(ProductTargetingHistory(**d))
        return out

    def toTargetingHistoryList(self):
        out = list()
        for ad in self.testcase.ad.list:
            if ad != "targeting":
                continue

            for o in self._toBiddingHistoryList(ad):
                d = self.asdict(o)
                d["targeting_id"] = ad.id
                out.append(TargetingHistory(**d))
        return out


if __name__ == "__main__":
    from simulate import LatentParams, Simulator
    from pprint import pprint as print

    params = LatentParams(3000, 0.1, 0.1, 1000)
    testcase = TestCase(
        id=1,
        name="test",
        description="dd",
        ad_type="targeting",
        budget=10000,
        mode="goal",
        purpose="roas",
        target_value=1,
        params=params,
        med_winning_price=50,
        default_bid=50,
    )
    s = Simulator(testcase)
    for _ in range(30):
        s.step()

    c = Converter(testcase, s.performances)
    print(c.toAdvertisingAccount())
    print(c.toPortfolio())
    print(c.toCampaign())
    print(c.toAdGroup())
    print(c.toKeyword())
    print(c.toProductTargeting())
    print(c.toTargeting())
    print(c._toDailyList())
    print(c.toDailyKeywordList())
    print(c.toDailyProductTargetingList())
    print(c.toDailyTargetingList())
    print(c.toDailyAdGroupList())
    print(c.toDailyCampaignList())
    print(c.toOptimizationAdSetting())
    print(c.toOptimizationSetting())
    print(c.toBiddingUnitInfo())
    print(c.toBiddingAdPerformanceList())

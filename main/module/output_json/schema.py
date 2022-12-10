from marshmallow import Schema, fields, post_load


class AllArgConstructorModelBidding:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class BaseSchema(Schema):
    @post_load
    def build(self, data, **kwargs):
        return AllArgConstructorModelBidding(**data)


class UnitBiddingPriceSchema(BaseSchema):

    ad_id = fields.Str(required=True)
    is_paused = fields.Boolean(required=True)
    bidding_price = fields.Float(required=True)


class CampaignOutputSchema(BaseSchema):

    campaign_id = fields.Str(required=True)
    daily_budget = fields.Int(allow_none=True)
    ads = fields.List(fields.Nested(UnitBiddingPriceSchema), allow_none=True)


class UnitSchema(fields.Field):
    campaigns = fields.List(fields.Nested(CampaignOutputSchema), required=True)


class OutputRootSchema(BaseSchema):

    result_type = fields.Int(required=True)
    unit = UnitSchema()
    is_ml_enabled = fields.Boolean(required=True)

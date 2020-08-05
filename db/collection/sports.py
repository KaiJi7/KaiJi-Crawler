import mongoengine


class TeamInfo(mongoengine.Document):
    name = mongoengine.StringField(required=True)
    score = mongoengine.IntField(min_value=0)


class Side(mongoengine.Document):
    meta = {'allow_inheritance': True}
    host = mongoengine.FloatField()
    guest = mongoengine.FloatField()


class SidePrediction(Side):
    major = mongoengine.BooleanField()


class GambleInfo(mongoengine.Document):
    class TotalPoint(mongoengine.Document):
        class ThresholdResponse(mongoengine.Document):
            meta = {'allow_inheritance': True}
            under = mongoengine.FloatField()
            over = mongoengine.FloatField()

        class ThresholdPrediction(ThresholdResponse):
            meta = {'allow_inheritance': True}
            major = mongoengine.BooleanField()

        threshold = mongoengine.IntField()
        response = mongoengine.ReferenceField(ThresholdResponse)
        judgement = mongoengine.StringField("^[(under)|(over)]$")
        prediction = mongoengine.ReferenceField(ThresholdPrediction)

    class SpreadPoint(mongoengine.Document):

        host = mongoengine.FloatField()
        guest = mongoengine.FloatField()
        response = mongoengine.ReferenceField(Side)
        judgement = mongoengine.StringField("^[(guest)|(host)]$")
        prediction = mongoengine.ReferenceField(SidePrediction)

    class Original(mongoengine.Document):
        response = mongoengine.ReferenceField(Side)
        judgement = mongoengine.StringField("^[(guest)|(host)]$")
        prediction = mongoengine.ReferenceField(SidePrediction)

    total_point = mongoengine.ReferenceField(TotalPoint)
    spread_point = mongoengine.ReferenceField(SpreadPoint)
    original = mongoengine.ReferenceField(Original)


class Judgement(mongoengine.EmbeddedDocumentField):
    total_point = mongoengine.StringField("^[(under)|(over)]$")
    spread_point = mongoengine.StringField("^[(guest)|(host)]$")
    original = mongoengine.StringField("^[(guest)|(host)]$")


class SportsData(mongoengine.Document):
    game_id = mongoengine.StringField(required=True)
    game_time = mongoengine.DateTimeField()
    gamble_id = mongoengine.StringField()
    sports_type = mongoengine.StringField(required=True)
    guest = mongoengine.ReferenceField(TeamInfo)
    host = mongoengine.ReferenceField(TeamInfo)
    gamble_info = mongoengine.ReferenceField(GambleInfo)

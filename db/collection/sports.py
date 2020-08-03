import mongoengine


class TeamInfo(mongoengine.EmbeddedDocumentField):
    name = mongoengine.StringField(required=True)
    score = mongoengine.IntField(min_value=0)


class Side(mongoengine.EmbeddedDocumentField):
    host = mongoengine.FloatField()
    guest = mongoengine.FloatField()


class SidePrediction(Side):
    major = mongoengine.BooleanField()


class GambleInfo(mongoengine.EmbeddedDocumentField):
    class TotalPoint(mongoengine.EmbeddedDocumentField):
        class ThresholdResponse(mongoengine.EmbeddedDocumentField):
            under = mongoengine.FloatField()
            over = mongoengine.FloatField()

        class ThresholdPrediction(ThresholdResponse):
            major = mongoengine.BooleanField()

        threshold = mongoengine.IntField()
        response = mongoengine.EmbeddedDocumentField(ThresholdResponse)
        judgement = mongoengine.StringField("^[(under)|(over)]$")
        prediction = mongoengine.EmbeddedDocumentField(ThresholdPrediction)

    class SpreadPoint(mongoengine.EmbeddedDocumentField):

        host = mongoengine.FloatField()
        guest = mongoengine.FloatField()
        response = mongoengine.EmbeddedDocumentField(Side)
        judgement = mongoengine.StringField("^[(guest)|(host)]$")
        prediction = mongoengine.EmbeddedDocumentField(SidePrediction)

    class Original(mongoengine.EmbeddedDocumentField):
        response = mongoengine.EmbeddedDocumentField(Side)
        judgement = mongoengine.StringField("^[(guest)|(host)]$")
        prediction = mongoengine.EmbeddedDocumentField(SidePrediction)

    total_point = mongoengine.EmbeddedDocumentField(TotalPoint)
    spread_point = mongoengine.EmbeddedDocumentField(SpreadPoint)
    original = mongoengine.EmbeddedDocumentField(Original)


class Judgement(mongoengine.EmbeddedDocumentField):
    total_point = mongoengine.StringField("^[(under)|(over)]$")
    spread_point = mongoengine.StringField("^[(guest)|(host)]$")
    original = mongoengine.StringField("^[(guest)|(host)]$")


class SportsData(mongoengine.Document):
    game_id = mongoengine.StringField(required=True)
    game_time = mongoengine.DateTimeField()
    gamble_id = mongoengine.StringField()
    sports_type = mongoengine.StringField(required=True)
    guest = mongoengine.EmbeddedDocumentField(TeamInfo)
    host = mongoengine.EmbeddedDocumentField(TeamInfo)
    gamble_info = mongoengine.EmbeddedDocumentField(GambleInfo)

import mongoengine


class TeamInfo(mongoengine.EmbeddedDocumentField):
    name = mongoengine.StringField(required=True)
    score = mongoengine.IntField(min_value=0)


class GambleInfo(mongoengine.EmbeddedDocumentField):
    class TotalPoint(mongoengine.EmbeddedDocumentField):
        threshold = mongoengine.IntField
        response
    class SpreadPoint(mongoengine.EmbeddedDocumentField):
        host = mongoengine.FloatField
        guest = mongoengine.FloatField


class SportsData(mongoengine.Document):
    game_id = mongoengine.StringField(required=True)
    game_time = mongoengine.DateTimeField()
    gamble_id = mongoengine.StringField()
    gamble_type = mongoengine.StringField(required=True)
    guest = mongoengine.EmbeddedDocumentField(TeamInfo)
    host = mongoengine.EmbeddedDocumentField(TeamInfo)

import mongoengine


class TeamInfo(mongoengine.Document):
    name = mongoengine.StringField(required=True)
    score = mongoengine.IntField(min_value=0)


class Side(mongoengine.Document):
    meta = {"allow_inheritance": True}
    host = mongoengine.FloatField()
    guest = mongoengine.FloatField()


class Prediction(mongoengine.Document):
    percentage = mongoengine.FloatField()
    population = mongoengine.IntField()


class SidePrediction(mongoengine.Document):
    guest = mongoengine.ReferenceField(Prediction)
    host = mongoengine.ReferenceField(Prediction)
    major = mongoengine.BooleanField()


class GambleInfo(mongoengine.Document):
    class TotalPoint(mongoengine.Document):
        class ThresholdResponse(mongoengine.Document):
            meta = {"allow_inheritance": True}
            under = mongoengine.FloatField()
            over = mongoengine.FloatField()

        class ThresholdPrediction(mongoengine.Document):
            meta = {"allow_inheritance": True}
            under = mongoengine.ReferenceField(Prediction)
            over = mongoengine.ReferenceField(Prediction)
            major = mongoengine.BooleanField()

        threshold = mongoengine.FloatField()
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
    # game_id = mongoengine.StringField(required=True)
    game_time = mongoengine.DateTimeField()
    gamble_id = mongoengine.StringField()
    game_type = mongoengine.StringField(required=True)
    # guest = mongoengine.ReferenceField(TeamInfo)
    # host = mongoengine.ReferenceField(TeamInfo)
    guest = mongoengine.DictField()
    host = mongoengine.DictField()
    gamble_info = mongoengine.ReferenceField(GambleInfo)

    meta = {
        "indexes": [
            "game_time",
            "game_type",
            {
                "fields": ["game_time", "game_type", "guest.name", "host.name"],
                "unique": True,
            },
        ]
    }

    def __str__(self):
        return f'game type: {self.game_type}, gamble id: {self.gamble_id}'


template = {
    # 'game_id': None,
    "game_time": None,
    "gamble_id": None,
    "game_type": None,
    "guest": {"name": None, "score": None,},
    "host": {"name": None, "score": None,},
    "gamble_info": {
        "total_point": {
            "threshold": None,
            "response": {"under": None, "over": None},
            "judgement": None,
            "prediction": {
                "under": {"percentage": None, "population": None},
                "over": {"percentage": None, "population": None},
                "major": None,
            },
        },
        "spread_point": {
            "guest": None,
            "host": None,
            "response": {"guest": None, "host": None,},
            "judgement": None,
            "prediction": {
                "guest": {"percentage": None, "population": None},
                "host": {"percentage": None, "population": None},
                "major": None,
            },
        },
        "original": {
            "response": {"guest": None, "host": None,},
            "judgement": None,
            "prediction": {
                "guest": {"percentage": None, "population": None},
                "host": {"percentage": None, "population": None},
                "major": None,
            },
        },
    },
}

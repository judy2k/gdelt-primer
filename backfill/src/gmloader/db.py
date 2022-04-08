from pymongo import MongoClient


class DB:
    def __init__(self, uri):
        self.db = MongoClient(uri).get_default_database()

    def already_inserted(self, download_id):
        """Return True if the download_id is already uploaded."""
        return (
            self.db.get_collection("insertLog").find_one(
                {
                    "_id": download_id,
                }
            )
            is not None
        )

    def set_inserted(self, download_id):
        return self.db.get_collection("insertLog").insert_one(
            {
                "_id": download_id,
            }
        )

    def convert(self, download_id):
        self.db.get_collection("eventsCSV").aggregate(
            [
                {
                    "$match": {"downloadId": download_id},
                },
                {
                    "$project": {
                        "GlobalEventId": 1,
                        "Day": 1,
                        "MonthYear": 1,
                        "Year": 1,
                        "FractionDate": 1,
                        "Actor1": {
                            "Code": "$Actor1Code",
                            "Name": "$Actor1Name",
                            "CountryCode": "$Actor1CountryCode",
                            "KnownGroupCode": "$Actor1KnownGroupCode",
                            "EthnicCode": "$Actor1EthnicCode",
                            "Religion1Code": "$Actor1Religion1Code",
                            "Religion2Code": "$Actor1Religion2Code",
                            "Type1Code": "$Actor1Type1Code",
                            "Type2Code": "$Actor1Type2Code",
                            "Type3Code": "$Actor1Type3Code",
                            "Geo_Type": "$Actor1Geo_Type",
                            "Geo_Fullname": "$Actor1Geo_Fullname",
                            "Geo_CountryCode": "$Actor1Geo_CountryCode",
                            "Geo_ADM1Code": "$Actor1Geo_ADM1Code",
                            "Geo_ADM2Code": "$Actor1Geo_ADM2Code",
                            "Location": {
                                "type": "Point",
                                "coordinates": [
                                    {
                                        "$convert": {
                                            "input": "$Actor1Geo_Long",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                    {
                                        "$convert": {
                                            "input": "$Actor1Geo_Lat",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                ],
                            },
                            "Geo_FeatureID": "$Actor1Geo_FeatureID",
                        },
                        "Actor2": {
                            "Code": "$Actor2Code",
                            "Name": "$Actor2Name",
                            "CountryCode": "$Actor2CountryCode",
                            "KnownGroupCode": "$Actor2KnownGroupCode",
                            "EthnicCode": "$Actor2EthnicCode",
                            "Religion1Code": "$Actor2Religion1Code",
                            "Religion2Code": "$Actor2Religion2Code",
                            "Type1Code": "$Actor2Type1Code",
                            "Type2Code": "$Actor2Type2Code",
                            "Type3Code": "$Actor2Type3Code",
                            "Geo_Type": "$Actor2Geo_Type",
                            "Geo_Fullname": "$Actor2Geo_Fullname",
                            "Geo_CountryCode": "$Actor2Geo_CountryCode",
                            "Geo_ADM1Code": "$Actor2Geo_ADM1Code",
                            "Geo_ADM2Code": "$Actor2Geo_ADM2Code",
                            "Location": {
                                "type": "Point",
                                "coordinates": [
                                    {
                                        "$convert": {
                                            "input": "$Actor2Geo_Long",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                    {
                                        "$convert": {
                                            "input": "$Actor2Geo_Lat",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                ],
                            },
                            "Geo_FeatureID": "$Actor2Geo_FeatureID",
                        },
                        "Action": {
                            "Geo_Type": "$ActionGeo_Type",
                            "Geo_Fullname": "$ActionGeo_Fullname",
                            "Geo_CountryCode": "$ActionGeo_CountryCode",
                            "Geo_ADM1Code": "$ActionGeo_ADM1Code",
                            "Geo_ADM2Code": "$ActionGeo_ADM2Code",
                            "Location": {
                                "type": "Point",
                                "coordinates": [
                                    {
                                        "$convert": {
                                            "input": "$ActionGeo_Long",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                    {
                                        "$convert": {
                                            "input": "$ActionGeo_Lat",
                                            "to": "double",
                                            "onError": 0,
                                            "onNull": 0,
                                        }
                                    },
                                ],
                            },
                            "Geo_FeatureID": "$ActionGeo_FeatureID",
                        },
                        "IsRootEvent": 1,
                        "EventCode": 1,
                        "EventBaseCode": 1,
                        "EventRootCode": 1,
                        "QuadClass": 1,
                        "GoldsteinScale": 1,
                        "NumMentions": 1,
                        "NumSources": 1,
                        "NumArticles": 1,
                        "AvgTone": 1,
                        "internal": {"downloadId": "$downloadId"},
                    },
                },
                {
                    "$merge": {"into": "recentEvents"},
                },
            ]
        )

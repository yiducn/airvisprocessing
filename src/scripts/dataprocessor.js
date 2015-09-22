/**
 * Created by yidu on 9/21/15.
 * procedure:
 * geoCodingGoogle()
 * geoEncodingBaidu()
 * preProcess() from pm25original to pm_preProcess
 * then generate pmdata_year month day
 */

//generate pmdata_year
db.pm_preProcess.aggregate([
    {
        $match:{
            time:{$gt:ISODate("2014-09-14T13:00:00.000Z")}
        }
    },
    {

        $group:{_id:{year:{$year:"$time"}, code:"$code"},
            year:{$first: {$year:"$time"}},
            time:{$first:"$time"},
            code:{$first:"$code"},
            aqi:{$avg:"$aqi"},
            co:{$avg:"$co"},
            no2:{$avg:"$no2"},
            o3:{$avg:"$o3"},
            pm10:{$avg:"$pm10"},
            pm25:{$avg:"$pm25"},
            so2:{$avg:"$so2"}
        }
    },
    {
        $out:"pmdata_year"
    }
]);

db.pm_preProcess.aggregate([
    {

        $group:{_id:{year:{$year:"$time"}, code:"$code"},
            year:{$first: {$year:"$time"}},
            time:{$first:"$time"},
            code:{$first:"$code"},
            aqi:{$avg:"$aqi"},
            co:{$avg:"$co"},
            no2:{$avg:"$no2"},
            o3:{$avg:"$o3"},
            pm10:{$avg:"$pm10"},
            pm25:{$avg:"$pm25"},
            so2:{$avg:"$so2"}
        }
    },
    {
        $out:"pmdata_year"
    }
]);

/**
 * 获得所有数据的按月平均
 */
db.pm_preProcess.aggregate([
    {

        $group:{_id:{month:{$month:"$time"}, year:{$year:"$time"}, code:"$code"},
            time:{$first:"$time"},
            month:{$first: {$month:"$time"}},
            code:{$first:"$code"},
            aqi:{$avg:"$aqi"},
            co:{$avg:"$co"},
            no2:{$avg:"$no2"},
            o3:{$avg:"$o3"},
            pm10:{$avg:"$pm10"},
            pm25:{$avg:"$pm25"},
            so2:{$avg:"$so2"}
        }
    },
    {
        $out:"pmdata_month"
    }
]);
//获取某个范围内的按月平均
db.pm_preProcess.aggregate([
    {
        $match:{
            time:{$gt:ISODate("2014-09-14T13:00:00.000Z")}
        }
    },
    {

        $group:{_id:{month:{$month:"$time"}, year:{$year:"$time"}, code:"$code"},
            time:{$first:"$time"},
            month:{$first: {$month:"$time"}},
            code:{$first:"$code"},
            aqi:{$avg:"$aqi"},
            co:{$avg:"$co"},
            no2:{$avg:"$no2"},
            o3:{$avg:"$o3"},
            pm10:{$avg:"$pm10"},
            pm25:{$avg:"$pm25"},
            so2:{$avg:"$so2"}
        }
    },
    {
        $out:"pmdata_month"
    }
]);

//从preProcess中生成日平均
//注意,这里存在时区的bug
//https://jira.mongodb.org/browse/SERVER-6310
db.pm_preProcess.aggregate([
        {

            $group:{_id:{day:{$dayOfMonth:"$time"}, month:{$month:"$time"}, year:{$year:"$time"}, code:"$code"},
                time:{$first:"$time"},
                code:{$first:"$code"},
                aqi:{$avg:"$aqi"},
                co:{$avg:"$co"},
                no2:{$avg:"$no2"},
                o3:{$avg:"$o3"},
                pm10:{$avg:"$pm10"},
                pm25:{$avg:"$pm25"},
                so2:{$avg:"$so2"}
            }
        },
        {
            $out:"pmdata_day"
        }
    ]
    ,
    {
        allowDiskUse: true
    });

//从月平均中,根据指定城市获取结果,所有结果取平均
/**返回结果
 * { "_id" : ISODate("2015-08-01T00:00:00Z"), "time" : ISODate("2015-08-01T00:00:00Z"), "aqi" : 58.281292984869324, "co" : 0.674504126547455, "no2" : 23.97799174690509, "o3" : 50.82187070151306, "pm10" : 57.210453920220075, "pm25" : 32.600412654745526, "so2" : 11.246905089408529 }
 { "_id" : ISODate("2015-09-01T00:00:00Z"), "time" : ISODate("2015-09-01T00:00:00Z"), "aqi" : 56.630503144654085, "co" : 0.5525440251572327, "no2" : 28.827044025157235, "o3" : 57.30974842767296, "pm10" : 62.22169811320755, "pm25" : 28.54874213836478, "so2" : 12.54874213836478 }
 */
db.pmdata_month.aggregate(
    [
        {
            $match:{
                code:{$in:["2874A","..."]}
            }
        },
        {
            $group:{
                _id:{month:{$month:"$time"}, year:{$year:"$time"}},
                time:{$first:"$time"},
                aqi:{$avg:"$aqi"},
                co:{$avg:"$co"},
                no2:{$avg:"$no2"},
                o3:{$avg:"$o3"},
                pm10:{$avg:"$pm10"},
                pm25:{$avg:"$pm25"},
                so2:{$avg:"$so2"}

            }
        },
        {
            $sort:{
                time:1
            }
        }
    ]
);

db.pmdata_month.aggregate(
    [
        {
            $group:{
                _id:{month:{$month:"$time"}, year:{$year:"$time"}},
                time:{$first:"$time"},
                aqi:{$avg:"$aqi"},
                co:{$avg:"$co"},
                no2:{$avg:"$no2"},
                o3:{$avg:"$o3"},
                pm10:{$avg:"$pm10"},
                pm25:{$avg:"$pm25"},
                so2:{$avg:"$so2"}

            }
        },
        {
            $sort:{
                time:1
            }
        }
    ]
);

/**
 * 返回日平均
 */
db.pmdata_day.aggregate(
    [
        {
            $group:{
                _id:{day:{$dayOfYear:"$time"}, month:{$month:"$time"}, year:{$year:"$time"}},
                time:{$first:"$time"},
                aqi:{$avg:"$aqi"},
                co:{$avg:"$co"},
                no2:{$avg:"$no2"},
                o3:{$avg:"$o3"},
                pm10:{$avg:"$pm10"},
                pm25:{$avg:"$pm25"},
                so2:{$avg:"$so2"}

            }
        },
        {
            $sort:{
                time:1
            }
        }
    ]
);

/**
 * insert some missing data into original collection.
 * 因为:数据存储时一部分存在了mongodb集群,另一部分处在了16.70,存在一些交叉
 **/
db.pm2.find(
    {
        time_point:
        {
            $in:[/^2014-05-28/,/^2014-05-29/,/^2014-05-30/,/^2014-05-31/, /^2014-06/,/^2014-07/,/^2014-08/,/^2014-09/]
        }
    }
).forEach(function(doc){
    db.pm25inoriginal.insert(doc);
});

<?php
namespace Services;

use Carbon\Carbon;
use Helpers\MongoHelper;
use Helpers\RedisHelper;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * Created by PhpStorm.
 * User: alonexy
 * Date: 19/11/14
 * Time: 15:58
 */
class ForexOhlc
{
    public $log;
    protected $redis;
    protected $mongo;
    protected $JobSequenceService;
    protected $BlackHoleService;
    protected $JobName = "Lz";

    public function __construct()
    {
        $this->redis = RedisHelper::connections($this->getRedisConf())->Get();
        $this->mongo = MongoHelper::connections($this->getMongodbConf())->Get();
        $this->log   = new Logger("ForexOhlc");
        $this->log->pushHandler(new StreamHandler(__DIR__ . '/../logs/ForexOhlc.log', Logger::DEBUG));
        $this->JobSequenceService = JobSequenceService::getInstance($this->redis, 1);
        $this->BlackHoleService = new BlackHoleService();
    }

    public function getRedisConf()
    {
        return [
            'host' => getenv("FOREX_REDIS_HOST"),
            'port' => getenv("FOREX_REDIS_PORT"),
            'auth' => getenv("FOREX_REDIS_PASS"),
            'db_set' => getenv("FOREX_REDIS_DB"),
        ];
    }

    public function getMongodbConf()
    {
        $opts = [
            "uri" => getenv("FOREX_MONGO_URL"),
            "appname" => getenv("FOREX_MONGO_APP_NAME"),
            "authSource" => getenv("FOREX_MONGO_AUTH_DB"),
            "username" => getenv("FOREX_MONGO_AUTH_USER"),
            "password" => getenv("FOREX_MONGO_AUTH_PASS"),
        ];
        return $opts;
    }

    public function Run()
    {
        try {
            //持续堵塞10s直到获取到数据
            list($listKey, $retData) = $this->redis->brpop(["LZ_OHLC_JOB_LIST"], 10);
            if (!empty($retData)) {
                if (!$this->JobSequenceService->is_lock($this->JobName, $retData)) {
                    $lockRes = $this->JobSequenceService->lock($this->JobName, $retData);
                    if ($lockRes) {
                        $this->JobSequenceService->SetGroupDataHandleFun(
                            function ($retData) {
                                try {
                                    if (!empty($retData)) {
                                        $tickData = json_decode($retData, 1);
                                        //保存tick
                                        go(
                                            function () use ($tickData) {
                                                list($TickRes, $TicketKey) = $this->GetMongoCollectionName($tickData['symbol'], 'tick');
                                                if (!$TickRes) {
                                                    throw new \Exception("获取ticket mogoCollection失败 {$TicketKey}");
                                                }
                                                if($tickData['symbol'] == "XAUUSD"){
                                                       $this->BlackHoleService->pubData("public:{$tickData['symbol']}_tick",$tickData);
                                                }
                                                $this->SaveDataToMogo($TicketKey, $tickData);
                                            });
                                        foreach ($this->getCycles() as $cycle) {
                                            $this->OHLC($cycle, $tickData, $is_ask = false);
                                        }
                                    }
                                }
                                catch (\Exception $e) {
                                    throw new \Exception($e->getMessage());
                                }
                            });
                        $this->JobSequenceService->GroupDatasHandle($this->JobName, $retData);
                        $this->JobSequenceService->unlock($this->JobName, $retData); //unlock;
                    }
                }
            }
        }
        catch (\Exception $e) {
            $this->log->warning("Run Exception", [$e->getMessage(), $e->getFile(), $e->getCode()]);
        }
    }

    /**
     * @获取Kline周期——1min、5min、15min、30min、60min、240min、日线d、周线w、月线m 年线y
     * @return array
     */
    public function getCycles()
    {
        $list = [
            '1分钟' => 1,
            '5分钟' => 5,
            '15分钟' => 15,
            '30分钟' => 30,
            '1小时' => 60,
            '4小时' => 240,
            '日K' => 'day',
            '周K' => 'week',
            '月K' => 'month',
            '年K' => 'year'
        ];
        return $list;
    }

    /**
     * @获取存储key && 获取 上一个key
     * @param int $m
     * @param $Datetime
     * @param string $Symbol
     * @param bool|false $previousKey
     * @return string
     * @throws \Exception
     */
    public function getCacheKey($m = 0, $Datetime, $previousKey = false)
    {
        if (!in_array($m, $this->getCycles())) {
            throw new \Exception('周期错误');
        }
        $NowHour   = (int)bcadd(Carbon::parse($Datetime)->hour, 0);
        $NowMinute = (int)bcadd(Carbon::parse($Datetime)->minute, 0);
        if (is_numeric($m)) {
            if ($m < 60) {
                //1分钟
                if ($m == 1) {
                    if ($previousKey) {
                        return $Key = Carbon::parse($Datetime)->subMinute()->format('Y-m-d H:i');
                    }
                    return $Key = Carbon::parse($Datetime)->format('Y-m-d H:i');
                }
                $Minlist = [0];
                $nums    = bcdiv(60, $m);
                for ($i = 0; $i < $nums; $i++) {
                    $last = end($Minlist);
                    array_push($Minlist, (int)bcadd($m, $last));
                }
                foreach ($Minlist as $lk => $lv) {
                    if ($NowMinute <= $lv) {
                        $nowDate = Carbon::parse($Datetime);
                        if ($NowMinute == $lv) {
                            $nowDate->setTime($nowDate->hour, $lv, 0);
                        }
                        else {
                            $nowDate->setTime($nowDate->hour, $Minlist[$lk - 1], 0);
                        }
                        $Key = $nowDate->format('Y-m-d H:i');
                        if ($previousKey) {
                            $Key = $nowDate->subMinutes($m)->format('Y-m-d H:i');
                        }
                        return $Key;
                    }
                }
            }
            if ($m == 60) {
                $nowDate = Carbon::parse($Datetime);
                $nowDate->setMinute(0);
                $Key = date('Y-m-d H:i', $nowDate->getTimestamp());
                if ($previousKey) {
                    $Key = Carbon::parse(date('Y-m-d H:i', $nowDate->getTimestamp()))->subHour()->format('Y-m-d H:i');
                }
                return $Key;
            }
            if ($m > 60) {
                $HourList = [0];
                if ($m == 240) {
                    $nums = bcdiv(bcmul(24, 60), $m);
                    for ($i = 0; $i < $nums; $i++) {
                        $last = end($HourList);
                        array_push($HourList, (int)bcadd($last, 4));
                    }
                    foreach ($HourList as $lk => $lv) {
                        if ($NowHour <= $lv) {
                            $nowDate = Carbon::parse($Datetime);
                            if ($NowHour == $lv) {
                                $nowDate->setTime($lv, 0, 0);
                            }
                            else {
                                $nowDate->setTime($HourList[$lk - 1], 0, 0);
                            }
                            $Key = $nowDate->format('Y-m-d H:i');
                            if ($previousKey) {
                                $Key = $nowDate->subMinutes($m)->format('Y-m-d H:i');
                            }
                            return $Key;
                        }
                    }
                }
            }
        }
        if ($m == 'day') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->subDay()->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->format('Y-m-d');
        }
        if ($m == 'week') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->startOfWeek()->subWeek()->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->startOfWeek()->format('Y-m-d');
        }
        if ($m == 'month') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->startOfMonth()->subMonth()->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->startOfMonth()->format('Y-m-d');
        }
        if ($m == 'year') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->startOfYear()->subYear()->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->startOfYear()->format('Y-m-d');
        }
        return null;
    }

    /**
     * @name 开高低收数据处理
     */
    public function OHLC($m = 0, array $Ticket, $is_ask = true)
    {
        if (!in_array($m, $this->getCycles())) {
            throw new \Exception('周期错误');
        }
        if (empty($Ticket)) {
            throw new \Exception('Ticket is empty');
        }
        $TicketAsk = $Ticket['ask'];
        $TicketBid = $Ticket['bid'];
        if ($is_ask) {
            $stamp = '_ask';
            $Price = $TicketAsk;
        }
        else {
            $stamp = '_bid';
            $Price = $TicketBid;
        }
        $TicketSymbol       = $Ticket['symbol'];
        $TicketDate         = $Ticket['ctm_fmt'];
        $OhlcData           = [];
        $OhlcData['open']   = 0;
        $OhlcData['high']   = 0;
        $OhlcData['low']    = 0;
        $OhlcData['close']  = 0;
        $OhlcData['ctm']    = 0;
        $OhlcData['ctmfmt'] = '';
        list($gRes, $HashKey) = $this->GetMongoCollectionName($TicketSymbol, $m, $stamp);
        if (!$gRes) {
            throw new \Exception("生成mongoCollection失败 {$HashKey}");
        }
        $ThisKey            = $this->getCacheKey($m, $TicketDate);
        $OhlcData['ctm']    = (int)Carbon::parse($ThisKey)->getTimestamp();
        $OhlcData['ctmfmt'] = (string)$ThisKey;
        $OhlcData['volume'] = 0;
        //存在 当前的key
        list($res, $LocalOhlcData) = $this->ExitsDataToMogo($HashKey, $OhlcData['ctm']);
        if ($res) {
            $OhlcData['open']  = (double)$LocalOhlcData['open'];
            $OhlcData['close'] = (double)$LocalOhlcData['close'];
            $OhlcData['high']  = (double)max($LocalOhlcData['open'], $LocalOhlcData['high'], $LocalOhlcData['low'], $LocalOhlcData['close'], $Price);
            $OhlcData['low']   = (double)min($LocalOhlcData['open'], $LocalOhlcData['high'], $LocalOhlcData['low'], $LocalOhlcData['close'], $Price);
            $OhlcData['close'] = (double)$Price;
        }
        else {
            $OhlcData['open']  = (double)$Price;
            $OhlcData['high']  = (double)$Price;
            $OhlcData['low']   = (double)$Price;
            $OhlcData['close'] = (double)$Price;
        }
        if($TicketSymbol == "XAUUSD"){
            $this->BlackHoleService->pubData("public:{$TicketSymbol}_{$m}",$OhlcData);
        }
        $this->SaveDataToMogo($HashKey, $OhlcData);
    }

    public function ExitsDataToMogo($collectionName, $_id)
    {
        $collection = $this->mongo->forex->$collectionName;
        $res        = $collection->findOne(["_id" => $_id]);
        if (!empty($res)) {
            return [true, iterator_to_array($res)];
        }
        return [false, null];
    }

    public function GetMongoCollectionName($symbol, $cycle, $stamp = '')
    {
        if ($cycle == 'tick') {
            $date = date('Y-m-d');
            return [true, "Tick_{$symbol}_ticket_{$date}"];
        }
        if (!in_array($cycle, $this->getCycles())) {
            return [false, '周期值错误'];
        }
        return [true, "Kline_{$symbol}_{$cycle}_{$stamp}"];
    }

    public function SaveDataToMogo($collectionName, $arr)
    {
        try {
            if (!isset($arr['ctm'])) {
                throw new \Exception('ctm 字段不存在 _id改为ctm时间戳');
            }
            $arr['_id'] = $arr['ctm'];
            $collection = $this->mongo->forex->$collectionName;
            $collection->updateOne(["_id" => $arr['_id']], ['$set' => $arr], ["upsert" => true]);
        }
        catch (\Exception $e) {
            $this->log->err("SaveDataToMogo | " . $e->getMessage());
        }
    }
}
<?php
namespace Services;
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
    protected $JobSequenceService;
    protected $JobName = "Lz";

    public function __construct()
    {
        $this->redis = RedisHelper::connections($this->getRedisConf())->Get();
        $this->log = new Logger("ForexOhlc");
        $this->log->pushHandler(new StreamHandler(__DIR__.'/../logs/ForexOhlc.log',Logger::DEBUG));
        $this->JobSequenceService = JobSequenceService::getInstance($this->redis);
    }
    public function getRedisConf(){
        return [
            'host'=>'127.0.0.1',
            'port'=>'6379',
            'auth'=>'alonexy',
            'db_set'=>'0',
        ];
    }
    public function Run(){
        //持续堵塞10s直到获取到数据
        list($listKey,$retData) = $this->redis->brpop(["LZ_OHLC_JOB_LIST"],10);
        if(!$this->JobSequenceService->is_lock($this->JobName,$retData)){
            $lockRes = $this->JobSequenceService->lock($this->JobName,$retData);
            if($lockRes){
                $this->JobSequenceService->SetGroupDataHandleFun(function($retData){
                    try{
                        if(!empty($retData)){
                            $tickData = json_decode($retData,1);
                            var_dump($tickData);
                        }
                    }catch (\Exception $e){
                        throw new \Exception($e->getMessage());
                    }
                });
                $this->JobSequenceService->GroupDatasHandle($this->JobName,$retData);
            }
        }
        $this->JobSequenceService->unlock($this->JobName, $retData); //unlock;
    }
    /**
     * @获取Kline周期——1min、5min、15min、30min、60min、240min、日线d、周线w、月线m 年线y
     * @return array
     */
    public function getCycles()
    {
        return app('KlineService')->getCycles();
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
                        return $Key = Carbon::parse($Datetime)->subMinute(1)->format('Y-m-d H:i');
                    }
                    return $Key = Carbon::parse($Datetime)->format('Y-m-d H:i');
                }
                $Minlist = [0];
                $nums    = bcdiv(60, $m);
                for ($i = 0; $i < $nums; $i++) {
                    $last = last($Minlist);
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
                            $Key = $nowDate->subMinute($m)->format('Y-m-d H:i');
                        }
                        return $Key;
                    }
                }
            }
            if ($m == 60) {
                $nowDate         = Carbon::parse($Datetime);
                $nowDate->minute = 0;
                $Key             = date('Y-m-d H:i', $nowDate->timestamp);
                if ($previousKey) {
                    $Key = Carbon::parse(date('Y-m-d H:i', $nowDate->timestamp))->subHour(1)->format('Y-m-d H:i');
                }
                return $Key;
            }
            if ($m > 60) {
                $HourList = [0];
                if ($m == 240) {
                    $nums = bcdiv(bcmul(24, 60), $m);
                    for ($i = 0; $i < $nums; $i++) {
                        $last = last($HourList);
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
                                $Key = $nowDate->subMinute($m)->format('Y-m-d H:i');
                            }
                            return $Key;
                        }
                    }
                }
            }
        }
        if ($m == 'day') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->subDay(1)->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->format('Y-m-d');
        }
        if ($m == 'week') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->startOfWeek()->subWeek(1)->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->startOfWeek()->format('Y-m-d');
        }
        if ($m == 'month') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->startOfMonth()->subMonth(1)->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->startOfMonth()->format('Y-m-d');
        }
        if ($m == 'year') {
            if ($previousKey) {
                return $Key = Carbon::parse($Datetime)->startOfYear()->subYear(1)->format('Y-m-d');
            }
            return $Key = Carbon::parse($Datetime)->startOfYear()->format('Y-m-d');
        }
    }

}
<?php

namespace Services;

class JobSequenceService
{
    public  $prifix;
    private $redis;
    private static $_instance ;

    private function __construct($redis)
    {
        $this->redis  = $redis;
        $this->prifix = 'job_sequence:';

    }
    public static function getInstance($redis)
    {
        if(self::$_instance instanceof self)
        {
            return self::$_instance;
        }
        return self::$_instance = new  self($redis);
    }

    private $HandleFunc = null;
    public function CacheData($key,$data)
    {
        $this->redis->set($key,$data);
    }
    /**
     * 数据分组顺序拼接数据
     * @param array $jobData 任务数组数据
     * @param $reqIdKey 消息唯一ID key
     * @param $scoreKey  消息权重 key
     * @param array $groupKeys 消息 分组Key array
     * @param $jobName 使用的job
     * @return array
     */
    public function DataGroupJobSplicing(array $jobData, $reqIdKey, $scoreKey, array $groupKeys, $jobName)
    {
        try {
            $reqId      = $jobData[$reqIdKey];
            $score      = $jobData[$scoreKey];
            $unqiueArrs = [];
            foreach ($groupKeys as $gk) {
                $unqiueArrs[] = $jobData[$gk];
            }
            $message_key    = implode('_', $unqiueArrs);
            $hash_order_key = $this->prifix."{$jobName}:{$message_key}";
            $res            = $this->addJobData($hash_order_key, $reqId, $jobData, $score);
            return [true, $message_key];
        }
        catch (\RedisException $e) {
            return [false, $e->getMessage()];
        }
    }

    /**
     * 添加任务分组数据
     * @param $hash_order_key
     * @param $reqId
     * @param $jobData
     * @param $score
     * @return null
     */
    private function addJobData($hash_order_key, $reqId, $jobData, $score)
    {
        $options = array(
            'cas' => true,
            'retry' => 2,
        );
        $this->redis->transaction(
            $options, function ($tx) use ($hash_order_key, $reqId, $jobData, $score) {
            $tx->multi();   // With CAS, MULTI *must* be explicitly invoked.
            $tx->hset($hash_order_key, $reqId, json_encode($jobData));
            $tx->zadd($hash_order_key . '_s', $score, $reqId);
        });
        return $this->redis->zcard($hash_order_key . '_s');
    }
    public function zpop($key,$num=1)
    {
        $options = array(
            'cas' => true,
            'retry' => 2,
        );
        $limit  = max(0,($num-1));
        $arr = [];
        $this->redis->transaction(
            $options, function ($tx) use ($key,&$arr,$limit) {
            $tx->multi();   // With CAS, MULTI *must* be explicitly invoked.
            $arr = $tx->zrange($key,0,$limit);

            if(!empty($arr)){
                $tx->zrem($key,$arr);
            }
        });
        return $arr;
    }

    private function delJobData($hash_order_key, $value)
    {
        $options = array(
            'cas' => true,
            'retry' => 2,
        );
        $res     = $this->redis->transaction(
            $options, function ($tx) use ($hash_order_key, $value) {
            $tx->multi();   // With CAS, MULTI *must* be explicitly invoked.
            $tx->hdel($hash_order_key, $value);
            $tx->zrem("{$hash_order_key}_s", $value);
        });
        return $res;
    }

    /**
     * 设置数据处理方法
     * @param $function
     */
    public function SetGroupDataHandleFun($function)
    {
        $this->HandleFunc = $function;
    }

    /**
     * 分组数据批量处理
     * @param $jobName
     * @param $messageKey
     * @return array
     * @throws \Exception
     */
    public function GroupDatasHandle($jobName, $messageKey)
    {
        $hash_order_key = $this->prifix."{$jobName}:{$messageKey}";

        $reqIds = $this->redis->zrange($hash_order_key . '_s', 0, 999); //每次消耗1000条
        if (empty($reqIds)) {
            return true;
        }
        $MsgLists = $this->redis->hmget($hash_order_key, $reqIds);
        var_dump($this->HandleFunc);
        if (is_null($this->HandleFunc)) {
            throw new \Exception("SetHandleFun is nil");
        }
        foreach ($MsgLists as $k => $val) {
            try {
                $this->lockDelayed($jobName, $messageKey,2); //延长2s 防止处理时间过长导致锁超时
                call_user_func_array($this->HandleFunc, array(&$val));
            }
            catch (\Exception $e) {
                throw new \Exception($e->getMessage());
            }
            $this->delJobData($hash_order_key, $reqIds[$k]);
        }
        //递归消耗
        return $this->GroupDatasHandle($jobName, $messageKey);
    }

    /**
     * 获取job下分组数据key
     * @param $jobName
     * @return array
     */
    public function getJobGroupKeys($jobName)
    {
        $keys        = "$jobName:*";
        $ks          = $this->redis->keys($this->prifix.$keys);

        $msssageKeys = [];
        foreach ($ks as $k) {
            preg_match_all('/' . $this->prifix.$jobName . ':(.*)_s/', $k, $ma);
            if (isset($ma[1][0])) {
                $mKey = $ma[1][0];
                $msssageKeys[] = $mKey;
            }
        }
        return $msssageKeys;
    }

    /**
     * 获取是否存在锁
     * @param $jobName
     * @param $messageKey
     * @return mixed
     */
    public function is_lock($jobName, $messageKey)
    {
        return $this->redis->get($this->prifix."{$jobName}:lock:{$messageKey}");
    }

    /**
     * 任务处理时 锁
     * @param $jobName
     * @param $messageKey
     * @return mixed
     */
    public function lock($jobName, $messageKey,$expTime=30000)
    {
        $key = $this->prifix."{$jobName}:lock:{$messageKey}";
        $isLock = $this->redis->setnx($key,time()+$expTime);
        if($isLock)
        {
            return true;
        }
        else
        {
//            //加锁失败的情况下。判断锁是否已经存在，如果锁存在切已经过期，那么删除锁。进行重新加锁
//            $val = $this->redis->get($key);
//            if($val&&$val<time())
//            {
//                $this->redis->del($key);
//            }
//            return  $this->redis->setnx($key,time()+$expTime);
            return false; //靠进程解锁
        }
    }
    //延长锁时间
    public function lockDelayed($jobName, $messageKey,$expTime=3000)
    {
        $key = $this->prifix."{$jobName}:lock:{$messageKey}";
        return $this->redis->set($key,time()+$expTime);
    }
    /**
     * 处理完成后删除 锁
     * @param $jobName
     * @param $messageKey
     * @return mixed
     */
    public function unlock($jobName, $messageKey)
    {
         return $this->redis->del([$this->prifix."{$jobName}:lock:{$messageKey}"]);
    }

    /**
     * 获取请求ID
     * @return string
     */
    public function getReqId()
    {
        $date = date('Y-m-d');
        return $this->redis->incr($this->prifix."job_req_id:{$date}") . '_' . Functions::uuids();
    }
    private function __clone(){}
    private function __wakeup(){}
}
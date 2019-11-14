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

    public function __construct()
    {
        $this->log = new Logger("ForexOhlc");
        $this->log->pushHandler(new StreamHandler(__DIR__.'/../logs/ForexOhlc.log',Logger::DEBUG));
    }
    public function getRedisConf(){
        return [
            'host'=>'127.0.0.1',
            'port'=>'6379',
            'auth'=>'alonexy',
            'db_set'=>'1',
        ];
    }
    public function aaa(){
        $redis = RedisHelper::connections($this->getRedisConf())->Get();
        $redis->set("SwTask_test",1);
        $res = $redis->get("SwTask_test");
        var_dump($res);
        $this->log->info("service TEsT");
        echo "1 \n";
    }

}
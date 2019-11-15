<?php
/**
 * Created by PhpStorm.
 * User: alonexy
 * Date: 19/11/14
 * Time: 17:56
 */

namespace Helpers;


use Predis\Client;

class RedisHelper
{

    static $instance;
    private $ck;
    static $rc;

    private function __construct($conf)
    {
        $this->ck = $conf;
    }

    public function Get()
    {
        $redisConfs = $this->ck;
        $options    = [
            "scheme" => "tcp",
            "host" => $redisConfs['host'],
            "port" => $redisConfs['port'],
            'password' => $redisConfs['auth'],
            'database' => $redisConfs['db_set'],
        ];
        return new Client($options);
    }

    static public function connections($conf)
    {
        if (!self::$instance instanceof self) {
            self::$instance = new self($conf);
        }
        return self::$instance;
    }

    private function __clone() { }

    private function __wakeup() { }


}
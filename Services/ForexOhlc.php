<?php
namespace Services;
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

    public function aaa(){
        $this->log->info("service TEsT");
        echo "1 \n";
    }

}
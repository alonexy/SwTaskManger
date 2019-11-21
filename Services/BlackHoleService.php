<?php

namespace Services;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;


/**
 * 离心机推送websocket
 * Class CrawlService
 * @package App\Services
 */
class BlackHoleService
{
    private $Centrifugo;

    public function __construct(){
        $this->log   = new Logger("BlackHoleService");
        $this->log->pushHandler(new StreamHandler(__DIR__ . '/../logs/BlackHoleService.log', Logger::DEBUG));
        $this->Centrifugo = new \phpcent\Client(getenv('CENT_URL'),getenv('CENT_API_KEY'),getenv('CENT_SECRET'));
    }

    /**
     * 推送数据
     * @param $channel
     * @param $data
     * @return array
     */
    public function pubData($channel,array $data)
    {
        try{
            if(empty($channel)){
                throw new \Exception("channel is nil.");
            }
            if(empty($data) ){
                throw new \Exception("data is nil.");
            }
            $res = $this->Centrifugo->publish("{$channel}", $data);
            return [true,$res];
        }catch (\Exception $e){
            $this->log->err("== pubData Exception ==",[[$channel,$data],$e->getMessage(),$e->getLine(),$e->getCode()]);
           return [false,$e->getMessage()];
        }
    }

    /**
     * 发送广播 $channels 空格 间隔
     * @param $channels
     * @param $data
     * @return array
     */
    public function pubBroadcast($channels, $data)
    {
        try{
            if(empty($channel)){
                throw new \Exception("channels is nil.");
            }
            if(empty($data) ){
                throw new \Exception("data is nil.");
            }elseif(!is_array($data)){
                throw new \Exception("data Must be an array .");
            }
            $res =  $this->Centrifugo->broadcast($channels, $data);
            return [true,$res];
        }catch (\Exception $e){
            $this->log->err('== pubBroadcast Exception ==',[$e->getMessage(),$e->getLine(),$e->getCode()]);

            return [false,$e->getMessage()];
        }
    }
    /**
     * 获取授权token 默认 userId 空支持 anonymous
     * @param string $userId
     * @param int $exp
     * @param array $info
     * @return array
     */
    public function getToken($userId='',$exp=0,$info=[])
    {
        try{
            $token = $this->Centrifugo->generateConnectionToken($userId,$exp,$info);
            return [true,$token];
        }catch (\Exception $e){
           $this->log->err('== GetToken Exception ==',[[$userId,$exp,$info],$e->getMessage(),$e->getLine(),$e->getCode()]);
            return [false,$e->getMessage()];
        }
    }

    /**
     * 获取通道
     * @return mixed
     */
    public function getChannels(){
        try{
            $response = $this->Centrifugo->channels();
            return [true,Functions::object_array($response->result->channels)];
        }catch (\Exception $e){
            $this->log->err('== getChannels Exception ==',[$e->getMessage(),$e->getLine(),$e->getCode()]);
            return [false,$e->getMessage()];
        }
    }

    /**
     * 获取通道下的历史数据
     * @param $channel
     * @return array
     */
    public function getHistory($channel){
        try{
            $response = $this->Centrifugo->history($channel);
            return [true,$response];
        }catch (\Exception $e){
            $this->log->err('== getHistory Exception ==',[$e->getMessage(),$e->getLine(),$e->getCode()]);
            return [false,$e->getMessage()];
        }
    }

    public function getInfo(){
        return $this->Centrifugo->info();
    }
}
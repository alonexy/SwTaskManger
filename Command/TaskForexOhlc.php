<?php
/**
 * Created by PhpStorm.
 * User: alonexy
 * Date: 19/11/14
 * Time: 13:42
 */

namespace Command;


use Helpers\ServerHelper;
use Services\ForexOhlc;
use Swoole\Coroutine;
use Swoole\Process;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Formatter\OutputFormatterStyle;

class TaskForexOhlc extends Command
{
    protected $name = "TaskForexOhlc";
    protected static $defaultName = 'forex:ohlc';
    /**
     * 所有操作
     * @var array
     */
    protected $actions = [
        'start',
        'reload',
        'stop',
        'status'
    ];
    # PidFileDir
    protected $pidFilePath;

    public function __construct()
    {
        parent::__construct();
        $this->pidFilePath = __DIR__ . "/../bin/pids/TaskForexOhlc.pid";
    }

    /**
     * 配置
     */
    protected function configure()
    {
        $this->setDescription('Provide some commands to manage the Task.')
            ->setAliases(['lz:start', 'forex:start'])
            ->addArgument('action', InputArgument::REQUIRED, 'Set Action Run.')
            ->addArgument('worknum', InputArgument::OPTIONAL, 'Set Task at WorkNum Number.')
            ->addOption('daemonize', 'd', InputOption::VALUE_NONE, 'Set Start Task at daemonize Default No')
            ->setHelp('Task -> actions:' . json_encode($this->actions));
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $action = $input->getArgument('action');

        $SucOutputStyle = new OutputFormatterStyle('green');
        $ErrOutputStyle = new OutputFormatterStyle('white', 'red', ['bold', 'blink']);
        /**
         * 可用前景色和背景色有：black，red，green，  yellow，blue，magenta，cyan和white。
         *
         * 和可用的选项为：bold，underscore，blink，reverse （使在背景和前景颜色被交换的“反向视频”模式）和conceal（设定前景颜色为透明的，使键入的文本不可见-尽管它可被选择和复制;该选项通常在要求用户输入敏感信息时使用。
         */
        $output->getFormatter()->setStyle('success', $SucOutputStyle);
        $output->getFormatter()->setStyle('error', $ErrOutputStyle);

        switch ($action) {
            case "start":
                $this->Start($input, $output);
                break;
            case "reload":
                $this->Reload($input, $output);
                break;
            case "stop":
                $this->Stop($input, $output);
                break;
            case "status":
                $this->Status($input, $output);
                break;
            default:
                return $output->writeln("<error>action is Err must in ['start','reload', 'stop'] </error>");
        }
    }

    public function Start($input, $output)
    {
        $pid = ServerHelper::getPid($this->pidFilePath);
        if ($pid > 1) {
            $output->writeln("<error>{$this->name}  is  Running.</error>");
            return false;
        }
        $workerNum = $input->getArgument('worknum');
        $daemonize = $input->getOption('daemonize');
        if (!is_numeric($workerNum)) {
            $output->writeln("<error>--worknum :must numbeer. [Unkonw Err] </error>");
            return false;
        }
        $pool      = new \Swoole\Process\Pool($workerNum, 0, 0, true);

        $pool->on(
            "WorkerStart", function ($pool, $workerId) {
            echo "Worker#{$workerId} is started\n";
            $ForexOhlc = new ForexOhlc();
            $ForexOhlc->Run();
        });

        $pool->on(
            "WorkerStop", function ($pool, $workerId) {
            echo "Worker#{$workerId} is stopped\n";
        });
        if ($daemonize) {
            Process::daemon(true, false);
            $this->saveMasterPidToFile();
        }
        $pool->start();
    }

    public function saveMasterPidToFile()
    {
        $pid = getmypid();
        return file_put_contents($this->pidFilePath, $pid);
    }

    /**
     * 重载 httpServer
     * @param $input
     * @param $output
     * @return bool
     */
    private function Reload($input, $output)
    {
        $signal = SIGUSR2;
        $pid    = ServerHelper::getPid($this->pidFilePath);
        if ($pid < 1) {
            $output->writeln("<error>{$this->name} getPid is Err .</error>");
            return false;
        }
        if (!ServerHelper::isRunning($pid)) {
            return $output->writeln("<error>{$this->name} is Not Running. </error>");
        }
        // SIGUSR1(10):
        //  Send a signal to the management process that will smoothly restart all worker processes
        // SIGUSR2(12):
        //  Send a signal to the management process, only restart the task process

        ServerHelper::sendSignal($pid, $signal);
        return $output->writeln("<success>{$this->name} is Reload. </success>");
    }

    /**
     * 关闭 HttpServer
     * @param $input
     * @param $output
     * @return bool|\Helpers\bool
     */
    private function Stop($input, $output)
    {
        $pid = ServerHelper::getPid($this->pidFilePath);
        if ($pid < 1) {
            $output->writeln("<error>{$this->name} getPid is Err .</error>");
            return false;
        }
        if (!ServerHelper::isRunning($pid)) {
            $output->writeln("<error>{$this->name} is Not Running. </error>");
            return ServerHelper::removePidFile($this->pidFilePath);
        }
        // SIGTERM = 15
        if (ServerHelper::killAndWait($pid, SIGTERM)) {
            $output->writeln("<success>{$this->name} is Stop. </success>");
            return ServerHelper::removePidFile($this->pidFilePath);
        }
        $output->writeln("<error>{$this->name} Not Stop. [Unkonw Err] </error>");
        return false;
    }

    /**
     * 获取状态
     * @param $input
     * @param $output
     * @return bool
     */
    public function Status($input, $output)
    {
        $pid = ServerHelper::getPid($this->pidFilePath);
        if ($pid < 1) {
            $output->writeln("<error>{$this->name} is Not Running.</error>");
            return false;
        }
        if (!ServerHelper::isRunning($pid)) {
            return $output->writeln("<error>{$this->name} is Not Running. </error>");
        }
        return $output->writeln("<success>{$this->name} is Running. </success>");
    }
}
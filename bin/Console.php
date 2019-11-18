#!/usr/bin/env php
<?php

require __DIR__.'/../vendor/autoload.php';

use Symfony\Component\Console\Application;
date_default_timezone_set('PRC');
$dotenv = new Dotenv\Dotenv(__DIR__.'/../');
$dotenv->load();

$app = new Application();
$app->add(new \Command\TaskForexOhlc());
$app->run();
#!/usr/bin/env php
<?php

require __DIR__.'/../vendor/autoload.php';

use Symfony\Component\Console\Application;

$app = new Application();
$app->add(new \Command\TaskForexOhlc());
$app->run();
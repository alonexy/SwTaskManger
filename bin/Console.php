#!/usr/bin/env php
<?php

require __DIR__.'/../vendor/autoload.php';

use Symfony\Component\Console\Application;

$app = new Application();

$app->add(new \Command\TaskProcess());
$app->add(new \Command\TaskDemo());
$app->run();
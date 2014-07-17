<?php
/**
 * Thrift初始化
 *
 * @author mikej
 * @link   http://mikej.sinaapp.com/
 */

define('HOST', '0.0.0.0');
define('PORT', '9090');

define('THRIFT_ROOT', realpath(__DIR__ . "/lib"));
define('GEN_DIR', realpath(__DIR__ . "/gen-php"));

# 初始化自动加载类
require_once THRIFT_ROOT . '/Thrift/ClassLoader/ThriftClassLoader.php';

use Thrift\ClassLoader\ThriftClassLoader;

$loader = new ThriftClassLoader();
$loader->registerNamespace('Thrift', THRIFT_ROOT);
$loader->registerDefinition("tService", GEN_DIR);
$loader->register();

use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TSocket;
use Thrift\Transport\THttpClient;
use Thrift\Transport\TBufferedTransport;
use Thrift\Exception\TException;

require_once GEN_DIR . '/Hbase.php';
require_once GEN_DIR . '/Types.php';

<?php
/**
 * 用户消息表user_message，只有一个列族message，用于测试。
 *
 */

require('./hbase.class.php');

$hbase = new hbase('user_message', 'message');

# add
$uid = "123456";
$rowKey = $uid;
for($i = 1; $i <= 10; $i++){
	sleep(1);
	$data = array(
		'title'      => 'hello',
		'content'    => 'hbase',
		'is_read'    => 0,
		'time'       => date("Y-m-d H:i:s"),
	);
	$hbase->add($rowKey, $data);
}

# edit
$data = array(
	'is_read'   => 1
);
$hbase->edit($rowKey, $data);

# search
// $data = $hbase->search($rowKey, "content");
$data = $hbase->search($rowKey);
var_dump($data);

# scan
$startRowKey = '0';
$endRowKey = '999999';
$nbRows = 10;
$data = $hbase->scan($startRowKey, $endRowKey, 1);
var_dump($data);

# filter scan
$where = array(
	'is_read' => array(
		'op'     => '=',
		'value'  => 0
	)
	'type' => array(
		'op'     => '=',
		'value'  => 1
	)
);

$data = $hbase->scanWithFilter($startRowKey, $endRowKey, $where, $nbRows);
var_dump($data);

# del
$hbase->del($rowKey);
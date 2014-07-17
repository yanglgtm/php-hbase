<?php
/**
 * Hbase操作类
 * 
 * 通过Thrift访问，Thrift版本：0.9.1。
 * 对Filter部分只做了简单的封装，完整的过滤器在这里有介绍：http://abloz.com/hbase/book.html#thrift
 *
 * @author mikej
 * @link   http://mikej.sinaapp.com/
 */

require('./init.php');

class hbase
{
    protected $table;
    protected $column_family;

    protected $socket;
    protected $transport;
    protected $client;

    protected $start_time;
    protected $end_time;

    protected $connect = FALSE;

    public function __construct($table, $column_family)
    {
        $this->start_time = $this->getMicrotime();
        $this->table = $table;
        $this->column_family = $column_family;

        try {
            $this->client = $this->connectHbase();
            $this->transport->open();
            $this->connect = TRUE;
        } catch (Exception $e) {
            $this->exceptionHander($e);
            exit;
        }
    }

    /**
     * 连接hbase
     *
     */
    public function connectHbase($sendTime = 10000, $recvTime = 20000)
    {
        $this->socket = new \Thrift\Transport\TSocket(HOST, PORT);
        $this->socket->setSendTimeout($sendTime);
        $this->socket->setRecvTimeout($recvTime);

        $this->transport = new \Thrift\Transport\TBufferedTransport($this->socket);
        $protocol = new \Thrift\Protocol\TBinaryProtocol($this->transport);
        return new HbaseClient($protocol);
    }

    /**
     * 添加数据
     *
     * @param $rowKey   行键
     * @param $data           
     */
    public function add($rowKey, array $data)
    {
        $mutations = array();

        foreach ($data as $qualifier => $content) {
            $mutations[] = new Mutation(array('column' => $this->column_family.':'.$qualifier, 'value' => $content));
        }

        $this->client->mutateRow($this->table, $rowKey, $mutations, array());
    }

    /**
     * 删除数据
     *
     */
    public function del($rowKey)
    {
        $this->client->deleteAllRow($this->table, $rowKey, array());
    }

    /**
     * 更新数据
     *
     */
    public function edit($rowKey, array $data)
    {
        $mutations = array();

        foreach ($data as $qualifier => $content) {
            $mutations[] = new Mutation(array('column' => $this->column_family.':'.$qualifier, 'value' => $content));
        }

        $this->client->mutateRow($this->table, $rowKey, $mutations, array());
    }

    /**
     * 查询一行数据或一个cell的数据
     *
     */
    public function search($rowKey, $qualifier = '')
    {
        if(!empty($qualifier)){
            $data = $this->client->get($this->table, $rowKey, $this->column_family.':'.$qualifier, array());
            if(!empty($data) && ($data[0] instanceof TCell)) {
                return $data[0];
            }
        }else{
            $data = $this->client->getRow($this->table, $rowKey, array());

            $result = array();
            if(!empty($data) && ($data[0] instanceof TRowResult)) {
                return $data[0];
            }
        }
    }

    /**
     * 扫描数据
     *
     * @param $startRow   起始行
     * @param $stopRow    结束行
     * @param $nbRows     返回数据条数
     */
    public function scan($startRow, $stopRow, $nbRows)
    {
        $col = array(
            'column' => $this->column_family
        );
        $scan = $this->client->scannerOpenWithStop($this->table, $startRow, $stopRow, $col, array());

        return $this->scanGetList($scan, $nbRows);
    }

    /**
     * 过滤扫描数据
     *
     * @param $startRow
     * @param $stopRow
     * @param $where
     * @param $operator    <, <=, =, !=, >, >=                                  比较运算符
     * @param $comparator  'binary','binaryprefix','regexstring','substring'    比较器
     * @param $nbRows
     */
    public function scanWithFilter($startRow, $stopRow, $where, $nbRows, $comparator="binary")
    {
        if(empty($where)){
            return $this->scan($startRow, $stopRow, $nbRows);
        }

        $filter = array();
        foreach ($where as $qualifier => $value) {
            $filter[] = "SingleColumnValueFilter('{$this->column_family}', '{$qualifier}' ,{$value['op']} , '{$comparator}:{$value['value']}')";
        }
        
        $filter_string = implode(" AND ", $filter);
        //echo $filter_string."\n";
        
        $scan_filter = new TScan();
        $scan_filter->filterString = $filter_string;
        $scan_filter->startRow = $startRow;
        $scan_filter->stopRow = $stopRow;
        $scan_filter->columns = array (
            'column' => $this->column_family
        );

        $scan = $this->client->scannerOpenWithScan($this->table, $scan_filter, array());
        
        return $this->scanGetList($scan, $nbRows);
    }

    /**
     * 获取扫描数据集
     * 
     */
    public function scanGetList($scan, $nbRows)
    {
        $data = $this->client->scannerGetList($scan, $nbRows);
        if(!empty($data)){
            return $data;
        }else{
            return array();
        }
    }

    /**
     * 设置表
     *
     */
    public function setTable($table)
    {
        $this->table = $table;
    }

    /**
     * 设置列族
     *
     */
    public function setColumnFamily($columnFamily)
    {
        $this->column_family = $columnFamily;
    }

    /**
     * 计算运行时间
     *
     */
    public function getMicrotime()
    {
        list($usec, $sec) = explode(" ", microtime());
        return ((float)$sec + (float)$usec);
    }

    protected function exceptionHander(Exception $e)
    {
        echo 'Error: ' . $e->getMessage();
    }

    public function __destruct()
    {
        if($this->connect){
            $this->transport->close();
        }
        $this->end_time = $this->getMicrotime();
        echo "\n" . ($this->end_time - $this->start_time);
    }

}
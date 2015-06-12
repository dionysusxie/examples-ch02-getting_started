package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader extends BaseRichSpout {

  private SpoutOutputCollector collector;
  private FileReader fileReader;
  private boolean completed = false;

  @Override
  public void ack(Object msgId) {
    System.out.println("OK:" + msgId);
  }

  @Override
  public void close() {}

  @Override
  public void fail(Object msgId) {
    System.out.println("FAIL:" + msgId);
  }

  /**
   * The only thing that the methods will do It is emit each file line
   */
  @Override
  public void nextTuple() {
    // The next tuple it is called forever, so if we have read out the file we
    // will wait and then return.
    if (completed) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Do nothing
      }
      return;
    }

    // Open the reader
    String str;
    BufferedReader reader = new BufferedReader(fileReader);
    try {
      while ((str = reader.readLine()) != null) {
        // By each line emit a new value with the line as a their
        this.collector.emit(new Values(str), str);
      }

    } catch (Exception e) {
      throw new RuntimeException("Error reading tuple", e);

    } finally {
      completed = true;
    }
  }

  /**
   * We will create the file and get the collector object
   */
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    try {
      this.fileReader = new FileReader(conf.get("wordsFile").toString());
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
    }
    this.collector = collector;
  }

  /**
   * Declare the output field "line"
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("line"));
  }
}

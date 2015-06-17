package grouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class MyGlobalGrouping implements CustomStreamGrouping {
  private List<Integer> target;

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    List<Integer> sorted = new ArrayList<Integer>(targetTasks);
    Collections.sort(sorted);
    this.target = Arrays.asList(sorted.get(0));
  }

  @Override
  public List<Integer> chooseTasks(int srcTaskId, List<Object> values) {
    return this.target;
  }
}

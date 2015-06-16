package grouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ModuleGrouping implements CustomStreamGrouping {
  private List<Integer> target;

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
      List<Integer> targetTasks) {
    List<Integer> sorted = new ArrayList<Integer>(targetTasks);
    Collections.sort(sorted);

    String tmp = "";
    for (Integer i : sorted) {
      tmp += " " + i;
    }
    System.out.println("--- target 1: " + tmp);

    this.target = Arrays.asList(sorted.get(0));
  }

  @Override
  public List<Integer> chooseTasks(int srcTaskId, List<Object> values) {
    System.out.println("chooseTasks() source task id = " + srcTaskId);
    return this.target;
  }

}

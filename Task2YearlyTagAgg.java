import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Task 2: aggregate Task1 outputs by (year, tag) and (optionally) get Top-K per year. */
public class Task2YearlyTagAgg {

    // 写死的 reducer 个数
    private static final int AGG_REDUCES  = 60;
    private static final int TOPK_REDUCES = 20;

    /** value for map->reduce: (count, views, score, answers) */
    public static class StatsWritable implements Writable {
        long count, views, score, answers;
        public StatsWritable() {}
        public StatsWritable(long c, long v, long s, long a) { count=c; views=v; score=s; answers=a; }
        @Override public void write(DataOutput out) throws IOException {
            out.writeLong(count); out.writeLong(views); out.writeLong(score); out.writeLong(answers);
        }
        @Override public void readFields(DataInput in) throws IOException {
            count = in.readLong(); views = in.readLong(); score = in.readLong(); answers = in.readLong();
        }
    }

    /** Mapper for aggregation: input line "tag year postId score ans views" */
    public static class AggMapper extends Mapper<LongWritable, Text, Text, StatsWritable> {
        private final Text outKey = new Text();
        private final StatsWritable outVal = new StatsWritable();
        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] p = line.split("\\s+");
            if (p.length < 6) return; // skip malformed
            String tag = p[0], year = p[1];
            long score = safeLong(p[3]);
            long ans   = safeLong(p[4]);
            long views = safeLong(p[5]);
            outKey.set(year + "\t" + tag);
            outVal.count = 1L; outVal.views = views; outVal.score = score; outVal.answers = ans;
            ctx.write(outKey, outVal);
        }
        private static long safeLong(String s) { try { return Long.parseLong(s); } catch (Exception e) { return 0L; } }
    }

    /** Reducer for aggregation: sum metrics and output with averages. */
    public static class AggReducer extends Reducer<Text, StatsWritable, Text, Text> {
        private final Text outVal = new Text();
        @Override
        protected void reduce(Text key, Iterable<StatsWritable> vals, Context ctx) throws IOException, InterruptedException {
            long c = 0, v = 0, s = 0, a = 0;
            for (StatsWritable w : vals) { c += w.count; v += w.views; s += w.score; a += w.answers; }
            double avgV = c == 0 ? 0.0 : (double) v / c;
            double avgS = c == 0 ? 0.0 : (double) s / c;
            double avgA = c == 0 ? 0.0 : (double) a / c;
            // 输出列：year tag count totalViews totalScore totalAnswers avgViews avgScore avgAnswers
            outVal.set(c + "\t" + v + "\t" + s + "\t" + a + "\t" +
                       String.format("%.6f", avgV) + "\t" +
                       String.format("%.6f", avgS) + "\t" +
                       String.format("%.6f", avgA));
            ctx.write(key, outVal);
        }
    }

    // -------- Optional Job 2: Top-K tags per year --------
    /** Mapper: read aggregation lines -> key=year, value="tag\tcount\tviews\tscore\tanswers" */
    public static class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text(), outVal = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] f = value.toString().split("\\t");
            if (f.length < 9) return; // need year tag and metrics
            String year = f[0], tag = f[1];
            String count = f[2], views = f[3], score = f[4], answers = f[5];
            outKey.set(year);
            outVal.set(tag + "\t" + count + "\t" + views + "\t" + score + "\t" + answers);
            ctx.write(outKey, outVal);
        }
    }

    /** Reducer: keep a min-heap of size K by (count desc, views desc). */
    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {
        private int K;
        @Override protected void setup(Context ctx) { K = ctx.getConfiguration().getInt("topk.N", 20); }
        @Override
        protected void reduce(Text year, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
            class Item { String tag; long cnt, views, score, answers;
                Item(String t,long c,long v,long s,long a){tag=t;cnt=c;views=v;score=s;answers=a;} }
            Comparator<Item> cmp = (i1, i2) -> (i1.cnt != i2.cnt) ? Long.compare(i1.cnt, i2.cnt)
                                                                  : Long.compare(i1.views, i2.views);
            PriorityQueue<Item> pq = new PriorityQueue<>(cmp);
            for (Text tv : vals) {
                String[] g = tv.toString().split("\\t");
                if (g.length < 5) continue;
                pq.offer(new Item(g[0], safeLong(g[1]), safeLong(g[2]), safeLong(g[3]), safeLong(g[4])));
                if (pq.size() > K) pq.poll();
            }
            ArrayList<Item> list = new ArrayList<>();
            while (!pq.isEmpty()) list.add(pq.poll());
            Collections.sort(list, (a,b)-> (a.cnt!=b.cnt)? Long.compare(b.cnt,a.cnt) : Long.compare(b.views,a.views));
            int rank = 1;
            for (Item it : list) {
                ctx.write(year, new Text(rank + "\t" + it.tag + "\t" + it.cnt + "\t" + it.views + "\t" + it.score + "\t" + it.answers));
                rank++;
            }
        }
        private static long safeLong(String s){ try { return Long.parseLong(s);} catch(Exception e){return 0L;}}
    }

    // -------- Driver --------
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage:\n  agg  <input> <output>\n  topk <input> <output> [K]");
            System.exit(2);
        }
        String mode = args[0];
        Configuration conf = new Configuration();
        // 让 reducer 更早启动
        conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.20f);

        if ("agg".equalsIgnoreCase(mode)) {
            Job job = Job.getInstance(conf, "Task2-YearlyTag-Aggregation");
            job.setJarByClass(Task2YearlyTagAgg.class);
            job.setMapperClass(AggMapper.class);
            job.setReducerClass(AggReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(StatsWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(AGG_REDUCES); // 写死 60
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } else if ("topk".equalsIgnoreCase(mode)) {
            if (args.length >= 4) conf.setInt("topk.N", Integer.parseInt(args[3]));
            Job job = Job.getInstance(conf, "Task2-YearlyTag-TopK");
            job.setJarByClass(Task2YearlyTagAgg.class);
            job.setMapperClass(TopKMapper.class);
            job.setReducerClass(TopKReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(TOPK_REDUCES); // 写死 20
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } else {
            System.err.println("Unknown mode: " + mode);
            System.exit(2);
        }
    }
}
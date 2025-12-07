import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1TagEmit {

    public static class MapOnly extends Mapper<LongWritable, Text, NullWritable, Text> {

        private static final Pattern ANGLED = Pattern.compile("<([^>]+)>");
        private final Text out = new Text();

        private static String getAttr(String line, String name) {
            String needle = name + "=\"";
            int i = line.indexOf(needle);
            if (i < 0) return null;
            int start = i + needle.length();
            int j = line.indexOf('"', start);
            if (j < 0) return null;
            return line.substring(start, j);
        }

        private static String unescapeBasic(String s) {
            if (s == null) return null;
            return s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&");
        }

        private static List<String> parseTags(String raw) {
            List<String> res = new ArrayList<>();
            if (raw == null || raw.isEmpty()) return res;

            String u = unescapeBasic(raw).trim();

            if (u.indexOf('<') >= 0 && u.indexOf('>') >= 0) {
                Matcher m = ANGLED.matcher(u);
                while (m.find()) {
                    String t = m.group(1).trim().toLowerCase();
                    if (!t.isEmpty()) res.add(t);
                }
            } else if (u.indexOf('|') >= 0) {
                for (String p : u.split("\\|")) {
                    String t = p.trim().toLowerCase();
                    if (!t.isEmpty()) res.add(t);
                }
            } else {
                String t = u.toLowerCase();
                if (!t.isEmpty()) res.add(t);
            }
            return res;
            }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.charAt(0) != '<') return;
            if (line.indexOf("<row ") != 0 && line.indexOf("<row") != 0) return;

            String postType = getAttr(line, "PostTypeId");
            if (!"1".equals(postType)) return;

            String id = getAttr(line, "Id");
            if (id == null) return;

            String creation = getAttr(line, "CreationDate");
            if (creation == null || creation.length() < 4) return;
            String year = creation.substring(0, 4);

            String score = getAttr(line, "Score");
            if (score == null) score = "0";

            String ans = getAttr(line, "AnswerCount");
            if (ans == null) ans = "0";

            String views = getAttr(line, "ViewCount");
            if (views == null) views = "0";

            String rawTags = getAttr(line, "Tags");
            List<String> tags = parseTags(rawTags);
            if (tags.isEmpty()) return;

            for (String t : tags) {
                out.set(t + " " + year + " " + id + " " + score + " " + ans + " " + views);
                ctx.write(NullWritable.get(), out);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task1TagEmit <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1-TagEmit");
        job.setJarByClass(Task1TagEmit.class);

        job.setMapperClass(MapOnly.class);
        job.setNumReduceTasks(0); // map-only

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
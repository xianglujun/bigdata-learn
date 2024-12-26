package org.hadoop.hive.learn.udaf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcatStrFunc extends AbstractGenericUDAFResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ConcatStrFunc.class);

    /**
     * 创建Evaluator对象
     *
     * @param info The types of the parameters. We need the type information to know
     *             which evaluator class to use.
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        LOG.info("获取Evaluator对象..");
        return new ConcatStrEvaluator();
    }

    public static class ConcatStrAggregationBuffer implements GenericUDAFEvaluator.AggregationBuffer {

        private StringBuilder concatStrBuilder = new StringBuilder();
        private Object monitor = new Object();
        private static final String SPLITTER = ",";

        public ConcatStrAggregationBuffer() {
            LOG.info("创建buffer对象..");
        }

        public ConcatStrAggregationBuffer(String str) {
            if (str != null && str.length() > 0) {
                concatStrBuilder.append(str);
            }
        }

        public String get() {
            return this.concatStrBuilder.toString();
        }

        public void add(String str) {
            if (str == null) {
                str = "";
            }
            synchronized (monitor) {
                this.concatStrBuilder.append(SPLITTER).append(str);
            }
        }

        public void reset() {
            synchronized (monitor) {
                this.concatStrBuilder.delete(0, concatStrBuilder.length());
            }
        }
    }

    public static class ConcatStrEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputObjectInspector;
        private ObjectInspector outputOI;

        public ConcatStrEvaluator() {

        }

        /**
         * 这里主要是用来初始化聚合函数，这里需要对传入的参数进行解析。这个函数在每个阶段都会被调用
         *
         * @param m          The mode of aggregation.
         * @param parameters The ObjectInspector for the parameters: In PARTIAL1 and COMPLETE
         *                   mode, the parameters are original data; In PARTIAL2 and FINAL
         *                   mode, the parameters are just partial aggregations (in that case,
         *                   the array will always have a single element).
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            LOG.info("初始化Evaluator#init()方法...");
            super.init(m, parameters);
            this.inputObjectInspector = (PrimitiveObjectInspector) parameters[0];

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // 这里是因为PARTIAL1和COMPLETE阶段都是输入的数据库的原始数据，所以他们的处理逻辑是一样的
            } else {
                // 这里主要是指代的时对PARTIAL2和FINAL阶段的数据，这里接受到的是上一个阶段计算出的结果，因此这里数据主要来自PARTIAL1和PARTIAL2的结果
            }

            // 这里是对输出数据的处理，保证我们输出的数据类型和我们需要的类型保持一致
            this.outputOI = ObjectInspectorFactory.getReflectionObjectInspector(
                    String.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA
            );
            return this.outputOI;
        }

        /**
         * 创建buffer对象，用于存储计算的中间结果
         *
         * @return
         * @throws HiveException
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LOG.info("调用Evaluator#getNewAggregationBuffer()方法...");
            return new ConcatStrAggregationBuffer();
        }

        /**
         * 重置buffer中的缓存数据
         *
         * @param agg
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            LOG.info("调用Evaluator#reset()方法...");
            ((ConcatStrAggregationBuffer) agg).reset();
        }

        /**
         * 遍历数据, 这里因为只会在partial1和COMPLETE中被调用，因此这里遍历的是从数据库中获取的原始数据
         *
         * @param agg
         * @param parameters The objects of parameters.
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            LOG.info("调用Evaluator#iterate()方法...");
            if (parameters == null || parameters.length == 0) {
                return;
            }

            ConcatStrAggregationBuffer buffer = (ConcatStrAggregationBuffer) agg;
            for (Object parameter : parameters) {
                buffer.add((String) this.inputObjectInspector.getPrimitiveJavaObject(parameter));
            }
        }

        /**
         * 这里调用的是一个阶段性的成果，用于在分布式计算中，将各个mapper的计算结果进行合并
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LOG.info("调用Evaluator#terminatePartial()");
            return this.terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            LOG.info("调用Evaluator#merge()");
            if (partial != null) {
                ConcatStrAggregationBuffer buffer = (ConcatStrAggregationBuffer) agg;
                // debug info
                if (this.inputObjectInspector == null) {
                    buffer.add("inputObjectInspector is null!");
                } else {
                    buffer.add((String) this.inputObjectInspector.getPrimitiveJavaObject(partial));
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LOG.info("调用Evaluator#terminate()");
            return ConcatStrAggregationBuffer.class.cast(agg).get();
        }
    }

    public static void main(String[] args) {
        ConcatStrEvaluator concatStrEvaluator = ReflectionUtils.newInstance(ConcatStrEvaluator.class, new Configuration());
        System.out.println(concatStrEvaluator);
    }

}

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamStatefulPythonFunctionRunner;
import org.apache.flink.streaming.api.typeutils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * doc.
 */
public class StatefulPythonFunctionOperator<OUT> extends AbstractOneInputPythonFunctionOperator<Row, OUT>
	implements ResultTypeQueryable<OUT>, Triggerable<Row, VoidNamespace> {

	protected static final String DATA_STREAM_STATEFUL_PYTHON_FUNCTION_URN =
		"flink:transform:datastream_stateful_function:v1";

	protected static final String DATA_STREAM_STATEFUL_MAP_FUNCTION_CODER_URN =
		"flink:coder:datastream:stateful_map_function:v1";
	protected static final String DATA_STREAM_STATEFUL_FLAT_MAP_FUNCTION_CODER_URN =
		"flink:coder:datastream:stateful_flatmap_function:v1";


	protected final DataStreamPythonFunctionInfo pythonFunctionInfo;
	private final TypeInformation runnerInputTypeInfo;
	private final TypeInformation runnerOutputTypeInfo;
	private final TypeInformation<OUT> outputTypeInfo;
	private final TypeInformation<Row> keyTypeInfo;
	private TypeSerializer runnerInputSerializer;
	private TypeSerializer runnerOutputSerializer;
	private TypeSerializer keyTypeSerializer;

	private transient TimerService timerservice;

	protected final Map<String, String> jobOptions;

	protected transient ByteArrayInputStreamWithPos bais;

	protected transient DataInputViewStreamWrapper baisWrapper;

	protected transient ByteArrayOutputStreamWithPos baos;

	protected transient DataOutputViewStreamWrapper baosWrapper;

	protected transient StreamRecordCollector streamRecordCollector;

	protected Row reusableInput;
	protected Row reusableTimerData;

	public StatefulPythonFunctionOperator(
		Configuration config,
		RowTypeInfo inputTypeInfo,
		TypeInformation<OUT> outputTypeInfo,
		DataStreamPythonFunctionInfo pythonFunctionInfo) {
		super(config);
		this.jobOptions = config.toMap();
		this.pythonFunctionInfo = pythonFunctionInfo;
		this.outputTypeInfo = outputTypeInfo;
		this.keyTypeInfo = new RowTypeInfo(inputTypeInfo.getTypeAt(0));
		this.keyTypeSerializer = PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(keyTypeInfo);
		// inputType: normal data/ timer data, timerType: proc/event time, keyData, real data
		this.runnerInputTypeInfo = Types.ROW(Types.INT, Types.LONG, this.keyTypeInfo, inputTypeInfo);
		this.runnerOutputTypeInfo = Types.ROW(Types.INT, Types.LONG, this.keyTypeInfo, outputTypeInfo);
	}

	@Override
	public void open() throws Exception {
		super.open();
		bais = new ByteArrayInputStreamWithPos();
		baisWrapper = new DataInputViewStreamWrapper(bais);

		baos = new ByteArrayOutputStreamWithPos();
		baosWrapper = new DataOutputViewStreamWrapper(baos);
		runnerInputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerInputTypeInfo);
		runnerOutputSerializer = PythonTypeUtils.TypeInfoToSerializerConverter
			.typeInfoSerializerConverter(runnerOutputTypeInfo);

		InternalTimerService<VoidNamespace> internalTimerService = getInternalTimerService("user-timers",
			VoidNamespaceSerializer.INSTANCE, this);

		this.streamRecordCollector = new StreamRecordCollector(output);
		timerservice = new SimpleTimerService(internalTimerService);
		reusableInput = new Row(4);
		reusableTimerData = new Row(4);
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return outputTypeInfo;
	}

	@Override
	public void onEventTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
		processTimer(false, timer);
		checkInvokeFinishBundleByCount();
	}

	@Override
	public void onProcessingTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
		processTimer(true, timer);
		checkInvokeFinishBundleByCount();
	}

	@Override
	public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
		return new BeamDataStreamStatefulPythonFunctionRunner(
			getRuntimeContext().getTaskName(),
			createPythonEnvironmentManager(),
			runnerInputTypeInfo,
			runnerOutputTypeInfo,
			DATA_STREAM_STATEFUL_PYTHON_FUNCTION_URN,
			getUserDefinedDataStreamFunctionsProto(),
			DATA_STREAM_STATEFUL_MAP_FUNCTION_CODER_URN,
			jobOptions,
			getFlinkMetricContainer(),
			getKeyedStateBackend(),
			keyTypeSerializer);
	}

	@Override
	public PythonEnv getPythonEnv() {
		return this.pythonFunctionInfo.getPythonFunction().getPythonEnv();
	}

	@Override
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		byte[] rawResult = resultTuple.f0;
		int length = resultTuple.f1;
		bais.setBuffer(rawResult, 0, length);
		Row runnerOutput = (Row) runnerOutputSerializer.deserialize(baisWrapper);
		if (runnerOutput.getField(0) != null) {
			registerTimer(runnerOutput);
		} else {
			streamRecordCollector.collect(runnerOutput.getField(3));
		}

	}

	@Override
	public void processElement(StreamRecord<Row> element) throws Exception {
		reusableInput.setField(0, null);
		reusableInput.setField(1, null);
		reusableInput.setField(3, element.getValue());
		runnerInputSerializer.serialize(reusableInput, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
		checkInvokeFinishBundleByCount();
		emitResults();
	}

	protected FlinkFnApi.UserDefinedDataStreamFunctions getUserDefinedDataStreamFunctionsProto() {
		FlinkFnApi.UserDefinedDataStreamFunctions.Builder builder = FlinkFnApi.UserDefinedDataStreamFunctions.newBuilder();
		builder.addUdfs(getUserDefinedDataStreamFunctionProto(pythonFunctionInfo));
		return builder.build();
	}

	private FlinkFnApi.UserDefinedDataStreamFunction getUserDefinedDataStreamFunctionProto(
		DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo) {
		FlinkFnApi.UserDefinedDataStreamFunction.Builder builder =
			FlinkFnApi.UserDefinedDataStreamFunction.newBuilder();
		builder.setPayload(com.google.protobuf.ByteString.copyFrom(
			dataStreamPythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		FlinkFnApi.TypeInfo.FieldType builtKeyFieldType = PythonTypeUtils.TypeInfoToProtoConverter
			.getFieldType(keyTypeInfo);
		builder.setKeyTypeInfo(PythonTypeUtils.TypeInfoToProtoConverter.toTypeInfoProto(builtKeyFieldType));
		return builder.build();
	}

	private void processTimer(boolean procTime, InternalTimer<Row, VoidNamespace> timer) throws Exception {
		long time = timer.getTimestamp();
		Row timerKey = timer.getKey();
		if (procTime) {
			reusableTimerData.setField(0, 0);
		} else {
			reusableTimerData.setField(0, 1);
		}

		reusableTimerData.setField(2, timerKey);

		reusableTimerData.setField(1, time);
		runnerInputSerializer.serialize(reusableTimerData, baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
		elementCount++;
	}

	private void registerTimer(Row row) {
		synchronized (getKeyedStateBackend()) {
			int type = (int) row.getField(0);
			long time = (long) row.getField(1);
			Object timerKey = row.getField(2);
			setCurrentKey(timerKey);
			if (type == 0) {
				this.timerservice.registerProcessingTimeTimer(time);
			} else {
				this.timerservice.registerEventTimeTimer(time);
			}
		}
	}
}

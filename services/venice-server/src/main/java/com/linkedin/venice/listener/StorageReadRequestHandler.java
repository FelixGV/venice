package com.linkedin.venice.listener;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.IngestionMetadataRetriever;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.chunking.BatchGetChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.cleaner.ResourceReadUsageTracker;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequest;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.OperationNotAllowedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.CurrentVersionRequest;
import com.linkedin.venice.listener.request.DictionaryFetchRequest;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.request.TopicPartitionIngestionContextRequest;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.ParallelMultiKeyResponseWrapper;
import com.linkedin.venice.listener.response.SingleGetResponseWrapper;
import com.linkedin.venice.listener.response.stats.ComputeResponseStatsWithSizeProfiling;
import com.linkedin.venice.listener.response.stats.MultiGetResponseStatsWithSizeProfiling;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.streaming.StreamingConstants;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.AvroRecordUtils;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/***
 * {@link StorageReadRequestHandler} will take the incoming read requests from router{@link RouterRequest}, and delegate
 * the lookup request to a thread pool {@link #executor}, which is being shared by all the requests. Especially, this
 * handler will execute parallel lookups for {@link MultiGetRouterRequestWrapper}.
 */
@ChannelHandler.Sharable
public class StorageReadRequestHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(StorageReadRequestHandler.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private final DiskHealthCheckService diskHealthCheckService;
  private final ThreadPoolExecutor executor;
  private final ThreadPoolExecutor computeExecutor;
  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlyStoreRepository metadataRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final IngestionMetadataRetriever ingestionMetadataRetriever;
  private final ReadMetadataRetriever readMetadataRetriever;
  private final Map<Utf8, Schema> computeResultSchemaCache;
  private final boolean fastAvroEnabled;
  private final Function<Schema, RecordSerializer<GenericRecord>> genericSerializerGetter;
  private final int parallelBatchGetChunkSize;
  private final VeniceServerConfig serverConfig;
  private final Map<String, PerStoreVersionState> perStoreVersionStateMap = new VeniceConcurrentHashMap<>();
  private final Map<String, StoreDeserializerCache<GenericRecord>> storeDeserializerCacheMap =
      new VeniceConcurrentHashMap<>();
  private final StorageEngineBackedCompressorFactory compressorFactory;
  private final Consumer<String> resourceReadUsageTracker;

  /**
   * The function handles below are used to drive the K/V size profiling, which is enabled (or not) by an immutable
   * config determined at the time we construct the {@link StorageReadRequestHandler}. This way, we don't need to
   * evaluate the config flag during every request.
   */
  private final IntFunction<MultiGetResponseWrapper> multiGetResponseProvider;
  private final IntFunction<ComputeResponseWrapper> computeResponseProvider;
  private final Function<MultiGetRouterRequestWrapper, CompletableFuture<ReadResponse>> multiGetHandler;

  private static class PerStoreVersionState {
    final StoreDeserializerCache<GenericRecord> storeDeserializerCache;
    AbstractStorageEngine storageEngine;

    public PerStoreVersionState(
        AbstractStorageEngine storageEngine,
        StoreDeserializerCache<GenericRecord> storeDeserializerCache) {
      this.storageEngine = storageEngine;
      this.storeDeserializerCache = storeDeserializerCache;
    }
  }

  private static class ReusableObjects {
    /**
     * When constructing a {@link BinaryDecoder}, we pass in this 16 bytes array because if we pass anything
     * less than that, it would end up getting discarded by the ByteArrayByteSource's constructor, a new byte
     * array created, and the content of the one passed in would be copied into the newly constructed one.
     * Therefore, it seems more efficient, in terms of GC, to statically allocate a 16 bytes array and keep
     * re-using it to construct decoders. Since we always end up re-configuring the decoder and not actually
     * using its initial value, it shouldn't cause any issue to share it.
     */
    private static final byte[] BINARY_DECODER_PARAM = new byte[16];

    // reuse buffer for rocksDB value object
    final ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);

    // LRU cache for storing schema->record map for object reuse of value and result record
    final LinkedHashMap<Schema, GenericRecord> valueRecordMap =
        new LinkedHashMap<Schema, GenericRecord>(100, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry<Schema, GenericRecord> eldest) {
            return size() > 100;
          }
        };

    final LinkedHashMap<Schema, GenericRecord> resultRecordMap =
        new LinkedHashMap<Schema, GenericRecord>(100, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry<Schema, GenericRecord> eldest) {
            return size() > 100;
          }
        };

    final BinaryDecoder binaryDecoder =
        AvroCompatibilityHelper.newBinaryDecoder(BINARY_DECODER_PARAM, 0, BINARY_DECODER_PARAM.length, null);

    final Map<String, Object> computeContext = new HashMap<>();
  }

  private final ThreadLocal<ReusableObjects> threadLocalReusableObjects = ThreadLocal.withInitial(ReusableObjects::new);

  public StorageReadRequestHandler(
      ThreadPoolExecutor executor,
      ThreadPoolExecutor computeExecutor,
      StorageEngineRepository storageEngineRepository,
      ReadOnlyStoreRepository metadataStoreRepository,
      ReadOnlySchemaRepository schemaRepository,
      IngestionMetadataRetriever ingestionMetadataRetriever,
      ReadMetadataRetriever readMetadataRetriever,
      DiskHealthCheckService healthCheckService,
      boolean fastAvroEnabled,
      boolean parallelBatchGetEnabled,
      int parallelBatchGetChunkSize,
      VeniceServerConfig serverConfig,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ResourceReadUsageTracker> optionalResourceReadUsageTracker) {
    this.executor = executor;
    this.computeExecutor = computeExecutor;
    this.storageEngineRepository = storageEngineRepository;
    this.metadataRepository = metadataStoreRepository;
    this.schemaRepository = schemaRepository;
    this.ingestionMetadataRetriever = ingestionMetadataRetriever;
    this.readMetadataRetriever = readMetadataRetriever;
    this.diskHealthCheckService = healthCheckService;
    this.fastAvroEnabled = fastAvroEnabled;
    this.genericSerializerGetter = fastAvroEnabled
        ? FastSerializerDeserializerFactory::getFastAvroGenericSerializer
        : SerializerDeserializerFactory::getAvroGenericSerializer;
    this.computeResultSchemaCache = new VeniceConcurrentHashMap<>();
    this.parallelBatchGetChunkSize = parallelBatchGetChunkSize;
    if (parallelBatchGetEnabled) {
      this.multiGetHandler = this::handleMultiGetRequestInParallel;
    } else {
      this.multiGetHandler = this::handleMultiGetRequest;
    }
    boolean keyValueProfilingEnabled = serverConfig.isKeyValueProfilingEnabled();
    if (keyValueProfilingEnabled) {
      this.multiGetResponseProvider =
          s -> new MultiGetResponseWrapper(s, new MultiGetResponseStatsWithSizeProfiling(s));
      this.computeResponseProvider = s -> new ComputeResponseWrapper(s, new ComputeResponseStatsWithSizeProfiling(s));
    } else {
      this.multiGetResponseProvider = MultiGetResponseWrapper::new;
      this.computeResponseProvider = ComputeResponseWrapper::new;
    }
    this.serverConfig = serverConfig;
    this.compressorFactory = compressorFactory;
    if (optionalResourceReadUsageTracker.isPresent()) {
      ResourceReadUsageTracker tracker = optionalResourceReadUsageTracker.get();
      this.resourceReadUsageTracker = tracker::recordReadUsage;
    } else {
      this.resourceReadUsageTracker = ignored -> {};
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    if (message instanceof RouterRequest) {
      RouterRequest request = (RouterRequest) message;
      this.resourceReadUsageTracker.accept(request.getResourceName());
      // Check before putting the request to the intermediate queue
      if (request.shouldRequestBeTerminatedEarly()) {
        // Try to make the response short
        context.writeAndFlush(
            new HttpShortcutResponse(
                VeniceRequestEarlyTerminationException.getMessage(request.getStoreName()),
                VeniceRequestEarlyTerminationException.getHttpResponseStatus()));
        return;
      }

      CompletableFuture<ReadResponse> responseFuture;
      switch (request.getRequestType()) {
        case SINGLE_GET:
          responseFuture = handleSingleGetRequest((GetRouterRequest) request);
          break;
        case MULTI_GET:
          responseFuture = this.multiGetHandler.apply((MultiGetRouterRequestWrapper) request);
          break;
        case COMPUTE:
          responseFuture = handleComputeRequest((ComputeRouterRequestWrapper) message);
          break;
        default:
          throw new VeniceException("Unknown request type: " + request.getRequestType());
      }

      responseFuture.whenComplete((response, throwable) -> {
        if (throwable == null) {
          response.setRCU(ReadQuotaEnforcementHandler.getRcu(request));
          if (request.isStreamingRequest()) {
            response.setStreamingResponse();
          }
          context.writeAndFlush(response);
        } else if (throwable instanceof VeniceNoStoreException) {
          VeniceNoStoreException e = (VeniceNoStoreException) throwable;
          String msg = "No storage exists for store: " + e.getStoreName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.error(msg, e);
          }
          HttpResponseStatus status = getHttpResponseStatus(e);
          context.writeAndFlush(new HttpShortcutResponse("No storage exists for: " + e.getStoreName(), status));
        } else if (throwable instanceof VeniceRequestEarlyTerminationException) {
          VeniceRequestEarlyTerminationException e = (VeniceRequestEarlyTerminationException) throwable;
          String msg = "Request timed out for store: " + e.getStoreName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.error(msg, e);
          }
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.REQUEST_TIMEOUT));
        } else if (throwable instanceof OperationNotAllowedException) {
          OperationNotAllowedException e = (OperationNotAllowedException) throwable;
          String msg = "METHOD_NOT_ALLOWED: " + e.getMessage();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.error(msg, e);
          }
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.METHOD_NOT_ALLOWED));
        } else {
          LOGGER.error("Exception thrown for {}", request.getResourceName(), throwable);
          HttpShortcutResponse shortcutResponse =
              new HttpShortcutResponse(throwable.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
          shortcutResponse.setMisroutedStoreVersion(checkMisroutedStoreVersionRequest(request));
          context.writeAndFlush(shortcutResponse);
        }
      });

    } else if (message instanceof HealthCheckRequest) {
      if (diskHealthCheckService.isDiskHealthy()) {
        context.writeAndFlush(new HttpShortcutResponse("OK", HttpResponseStatus.OK));
      } else {
        context.writeAndFlush(
            new HttpShortcutResponse(
                "Venice storage node hardware is not healthy!",
                HttpResponseStatus.INTERNAL_SERVER_ERROR));
        LOGGER.error(
            "Disk is not healthy according to the disk health check service: {}",
            diskHealthCheckService.getErrorMessage());
      }
    } else if (message instanceof DictionaryFetchRequest) {
      BinaryResponse response = handleDictionaryFetchRequest((DictionaryFetchRequest) message);
      context.writeAndFlush(response);
    } else if (message instanceof AdminRequest) {
      AdminResponse response = handleServerAdminRequest((AdminRequest) message);
      context.writeAndFlush(response);
    } else if (message instanceof MetadataFetchRequest) {
      try {
        MetadataResponse response = handleMetadataFetchRequest((MetadataFetchRequest) message);
        context.writeAndFlush(response);
      } catch (UnsupportedOperationException e) {
        LOGGER.warn(
            "Metadata requested by a storage node read quota not enabled store: {}",
            ((MetadataFetchRequest) message).getStoreName());
        context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.FORBIDDEN));
      }
    } else if (message instanceof CurrentVersionRequest) {
      ServerCurrentVersionResponse response = handleCurrentVersionRequest((CurrentVersionRequest) message);
      context.writeAndFlush(response);
    } else if (message instanceof TopicPartitionIngestionContextRequest) {
      TopicPartitionIngestionContextResponse response =
          handleTopicPartitionIngestionContextRequest((TopicPartitionIngestionContextRequest) message);
      context.writeAndFlush(response);
    } else {
      context.writeAndFlush(
          new HttpShortcutResponse(
              "Unrecognized object in StorageExecutionHandler",
              HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }
  }

  private HttpResponseStatus getHttpResponseStatus(VeniceNoStoreException e) {
    String topic = e.getStoreName();
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int version = Version.parseVersionFromKafkaTopicName(topic);
    Store store = metadataRepository.getStore(storeName);

    if (store == null || store.getCurrentVersion() != version) {
      return HttpResponseStatus.BAD_REQUEST;
    }

    // return SERVICE_UNAVAILABLE to kick off error retry in router when store version resource exists
    return HttpResponseStatus.SERVICE_UNAVAILABLE;
  }

  /**
   * Best effort check for the purpose of reporting misrouted store version metric when the request errors.
   */
  private boolean checkMisroutedStoreVersionRequest(RouterRequest request) {
    boolean misrouted = false;
    Store store = metadataRepository.getStore(request.getStoreName());
    if (store != null) {
      Version version = store.getVersion(Version.parseVersionFromVersionTopicName(request.getResourceName()));
      if (version == null) {
        misrouted = true;
      }
    }
    return misrouted;
  }

  private PerStoreVersionState getPerStoreVersionState(String storeVersion) {
    PerStoreVersionState s = perStoreVersionStateMap.computeIfAbsent(storeVersion, this::generatePerStoreVersionState);
    if (s.storageEngine.isClosed()) {
      /**
       * This scenario can happen if the last partition hosted on this server got dropped by Helix, for example in a
       * case where a store has a small number of partition-replicas spread across a larger number of servers, and a
       * rebalance happens. In such case, we refresh the storage engine by getting a reference to the latest one from
       * the {@link storageEngineRepository}.
       */
      s.storageEngine = getStorageEngineOrThrow(storeVersion);
    }
    return s;
  }

  private PerStoreVersionState generatePerStoreVersionState(String storeVersion) {
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
    AbstractStorageEngine storageEngine = getStorageEngineOrThrow(storeVersion);
    StoreDeserializerCache<GenericRecord> storeDeserializerCache = storeDeserializerCacheMap.computeIfAbsent(
        storeName,
        s -> new AvroStoreDeserializerCache<>(this.schemaRepository, s, this.fastAvroEnabled));
    return new PerStoreVersionState(storageEngine, storeDeserializerCache);
  }

  private AbstractStorageEngine getStorageEngineOrThrow(String storeVersion) {
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(storeVersion);
    if (storageEngine == null) {
      throw new VeniceNoStoreException(storeVersion);
    }
    return storageEngine;
  }

  public CompletableFuture<ReadResponse> handleSingleGetRequest(GetRouterRequest request) {
    final int queueLen = this.executor.getQueue().size();
    final long preSubmissionTimeNs = System.nanoTime();
    return CompletableFuture.supplyAsync(() -> {
      if (request.shouldRequestBeTerminatedEarly()) {
        throw new VeniceRequestEarlyTerminationException(request.getStoreName());
      }

      double submissionWaitTime = LatencyUtils.getElapsedTimeFromNSToMS(preSubmissionTimeNs);

      String topic = request.getResourceName();
      PerStoreVersionState perStoreVersionState = getPerStoreVersionState(topic);
      byte[] key = request.getKeyBytes();

      AbstractStorageEngine storageEngine = perStoreVersionState.storageEngine;
      boolean isChunked = storageEngine.isChunked();
      SingleGetResponseWrapper response = new SingleGetResponseWrapper();
      response.setCompressionStrategy(storageEngine.getCompressionStrategy());

      ValueRecord valueRecord =
          SingleGetChunkingAdapter.get(storageEngine, request.getPartition(), key, isChunked, response.getStats());
      response.setValueRecord(valueRecord);

      response.getStats().addKeySize(key.length);
      response.getStats().setStorageExecutionSubmissionWaitTime(submissionWaitTime);
      response.getStats().setStorageExecutionQueueLen(queueLen);

      return response;
    });
  }

  private CompletableFuture<ReadResponse> handleMultiGetRequestInParallel(MultiGetRouterRequestWrapper request) {
    List<MultiGetRouterRequestKeyV1> keys = request.getKeys();
    PerStoreVersionState perStoreVersionState = getPerStoreVersionState(request.getResourceName());
    AbstractStorageEngine storageEngine = perStoreVersionState.storageEngine;

    boolean isChunked = storageEngine.isChunked();

    int totalKeyNum = keys.size();
    int chunkCount = (int) Math.ceil((double) totalKeyNum / this.parallelBatchGetChunkSize);
    ParallelMultiKeyResponseWrapper<MultiGetResponseWrapper> responseWrapper = ParallelMultiKeyResponseWrapper
        .multiGet(chunkCount, this.parallelBatchGetChunkSize, this.multiGetResponseProvider);
    responseWrapper.setCompressionStrategy(storageEngine.getCompressionStrategy());

    CompletableFuture<Void>[] chunkFutures = new CompletableFuture[chunkCount];

    final int queueLen = this.executor.getQueue().size();
    final long preSubmissionTimeNs = System.nanoTime();
    for (int cur = 0; cur < chunkCount; ++cur) {
      final int finalCur = cur;
      chunkFutures[cur] = CompletableFuture.runAsync(() -> {
        double submissionWaitTime = LatencyUtils.getElapsedTimeFromNSToMS(preSubmissionTimeNs);

        if (request.shouldRequestBeTerminatedEarly()) {
          throw new VeniceRequestEarlyTerminationException(request.getStoreName());
        }

        int startPos = finalCur * this.parallelBatchGetChunkSize;
        int endPos = Math.min((finalCur + 1) * this.parallelBatchGetChunkSize, totalKeyNum);
        MultiGetResponseWrapper chunkOfResponse = responseWrapper.getChunk(finalCur);
        processMultiGet(
            startPos,
            endPos,
            keys,
            storageEngine,
            isChunked,
            chunkOfResponse,
            request.isStreamingRequest());

        chunkOfResponse.getStats().setStorageExecutionSubmissionWaitTime(submissionWaitTime);
      }, executor);
    }

    return CompletableFuture.allOf(chunkFutures).handle((v, e) -> {
      if (e != null) {
        throw new VeniceException(e);
      }

      responseWrapper.getChunk(0).getStats().setStorageExecutionQueueLen(queueLen);
      return responseWrapper;
    });
  }

  private void processMultiGet(
      int startPos,
      int endPos,
      List<MultiGetRouterRequestKeyV1> keys,
      AbstractStorageEngine storageEngine,
      boolean isChunked,
      MultiGetResponseWrapper responseWrapper,
      boolean isStreaming) {
    MultiGetResponseRecordV1 record;
    MultiGetRouterRequestKeyV1 key;
    for (int subChunkCur = startPos; subChunkCur < endPos; ++subChunkCur) {
      key = keys.get(subChunkCur);
      responseWrapper.getStats().addKeySize(key.getKeyBytes().remaining());
      record = BatchGetChunkingAdapter
          .get(storageEngine, key.partitionId, key.keyBytes, isChunked, responseWrapper.getStats());
      if (record == null) {
        if (isStreaming) {
          // For streaming, we would like to send back non-existing keys since the end-user won't know the status of
          // non-existing keys in the response if the response is partial.
          record = new MultiGetResponseRecordV1();
          // Negative key index to indicate the non-existing keys
          record.keyIndex = Math.negateExact(key.keyIndex);
          record.schemaId = StreamingConstants.NON_EXISTING_KEY_SCHEMA_ID;
          record.value = StreamingUtils.EMPTY_BYTE_BUFFER;
          responseWrapper.addRecord(record);
        }
      } else {
        record.keyIndex = key.keyIndex;
        responseWrapper.addRecord(record);
      }
    }

    // Trigger serialization
    responseWrapper.getResponseBody();
  }

  public CompletableFuture<ReadResponse> handleMultiGetRequest(MultiGetRouterRequestWrapper request) {
    final int queueLen = this.executor.getQueue().size();
    final long preSubmissionTimeNs = System.nanoTime();
    return CompletableFuture.supplyAsync(() -> {
      double submissionWaitTime = LatencyUtils.getElapsedTimeFromNSToMS(preSubmissionTimeNs);

      if (request.shouldRequestBeTerminatedEarly()) {
        throw new VeniceRequestEarlyTerminationException(request.getStoreName());
      }

      List<MultiGetRouterRequestKeyV1> keys = request.getKeys();
      PerStoreVersionState perStoreVersionState = getPerStoreVersionState(request.getResourceName());
      AbstractStorageEngine storageEngine = perStoreVersionState.storageEngine;
      MultiGetResponseWrapper responseWrapper = this.multiGetResponseProvider.apply(request.getKeyCount());
      responseWrapper.setCompressionStrategy(storageEngine.getCompressionStrategy());
      boolean isChunked = storageEngine.isChunked();
      processMultiGet(
          0,
          request.getKeyCount(),
          keys,
          storageEngine,
          isChunked,
          responseWrapper,
          request.isStreamingRequest());

      responseWrapper.getStats().setStorageExecutionSubmissionWaitTime(submissionWaitTime);
      responseWrapper.getStats().setStorageExecutionQueueLen(queueLen);
      return responseWrapper;
    }, executor);
  }

  private CompletableFuture<ReadResponse> handleComputeRequest(ComputeRouterRequestWrapper request) {
    if (!metadataRepository.isReadComputationEnabled(request.getStoreName())) {
      CompletableFuture failFast = new CompletableFuture();
      failFast.completeExceptionally(
          new OperationNotAllowedException(
              "Read compute is not enabled for the store. Please contact Venice team to enable the feature."));
      return failFast;
    }

    final int queueLen = this.computeExecutor.getQueue().size();
    final long preSubmissionTimeNs = System.nanoTime();
    return CompletableFuture.supplyAsync(() -> {
      if (request.shouldRequestBeTerminatedEarly()) {
        throw new VeniceRequestEarlyTerminationException(request.getStoreName());
      }

      double submissionWaitTime = LatencyUtils.getElapsedTimeFromNSToMS(preSubmissionTimeNs);

      SchemaEntry superSetOrLatestValueSchema = schemaRepository.getSupersetOrLatestValueSchema(request.getStoreName());
      SchemaEntry valueSchemaEntry = getComputeValueSchema(request, superSetOrLatestValueSchema);
      Schema resultSchema = getComputeResultSchema(request.getComputeRequest(), valueSchemaEntry.getSchema());
      RecordSerializer<GenericRecord> resultSerializer = genericSerializerGetter.apply(resultSchema);
      PerStoreVersionState storeVersion = getPerStoreVersionState(request.getResourceName());
      VeniceCompressor compressor = compressorFactory
          .getCompressor(storeVersion.storageEngine.getCompressionStrategy(), request.getResourceName());

      // Reuse the same value record and result record instances for all values
      ReusableObjects reusableObjects = threadLocalReusableObjects.get();
      GenericRecord reusableValueRecord =
          reusableObjects.valueRecordMap.computeIfAbsent(valueSchemaEntry.getSchema(), GenericData.Record::new);
      GenericRecord reusableResultRecord =
          reusableObjects.resultRecordMap.computeIfAbsent(resultSchema, GenericData.Record::new);
      reusableObjects.computeContext.clear();

      ComputeResponseWrapper response = computeResponseProvider.apply(request.getKeyCount());
      List<ComputeOperation> operations = request.getComputeRequest().getOperations();
      List<Schema.Field> operationResultFields = ComputeUtils.getOperationResultFields(operations, resultSchema);
      int hits = 0;
      for (ComputeRouterRequestKeyV1 key: request.getKeys()) {
        AvroRecordUtils.clearRecord(reusableResultRecord);
        GenericRecord result = computeResult(
            operations,
            operationResultFields,
            storeVersion,
            key,
            reusableValueRecord,
            valueSchemaEntry.getId(),
            compressor,
            response.getStats(),
            reusableObjects,
            reusableResultRecord);
        if (addComputationResult(response, key, result, resultSerializer, request.isStreamingRequest())) {
          hits++;
        }
      }
      incrementOperatorCounters(response.getStats(), operations, hits);
      response.getStats().setStorageExecutionSubmissionWaitTime(submissionWaitTime);
      response.getStats().setStorageExecutionQueueLen(queueLen);
      return response;
    }, computeExecutor);
  }

  private BinaryResponse handleDictionaryFetchRequest(DictionaryFetchRequest request) {
    ByteBuffer dictionary = ingestionMetadataRetriever.getStoreVersionCompressionDictionary(request.getResourceName());
    return new BinaryResponse(dictionary);
  }

  private MetadataResponse handleMetadataFetchRequest(MetadataFetchRequest request) {
    return readMetadataRetriever.getMetadata(request.getStoreName());
  }

  private ServerCurrentVersionResponse handleCurrentVersionRequest(CurrentVersionRequest request) {
    return readMetadataRetriever.getCurrentVersionResponse(request.getStoreName());
  }

  private Schema getComputeResultSchema(ComputeRequest computeRequest, Schema valueSchema) {
    Utf8 resultSchemaStr = (Utf8) computeRequest.getResultSchemaStr();
    Schema resultSchema = computeResultSchemaCache.get(resultSchemaStr);
    if (resultSchema == null) {
      resultSchema = new Schema.Parser().parse(resultSchemaStr.toString());
      // Sanity check on the result schema
      ComputeUtils.checkResultSchema(resultSchema, valueSchema, computeRequest.getOperations());
      computeResultSchemaCache.putIfAbsent(resultSchemaStr, resultSchema);
    }
    return resultSchema;
  }

  private SchemaEntry getComputeValueSchema(
      ComputeRouterRequestWrapper request,
      SchemaEntry superSetOrLatestValueSchema) {
    return request.getValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID
        ? schemaRepository.getValueSchema(request.getStoreName(), request.getValueSchemaId())
        : superSetOrLatestValueSchema;
  }

  /**
   * @return true if the result is not null, false otherwise
   */
  private boolean addComputationResult(
      ComputeResponseWrapper response,
      ComputeRouterRequestKeyV1 key,
      GenericRecord result,
      RecordSerializer<GenericRecord> resultSerializer,
      boolean isStreaming) {
    if (result != null) {
      long serializeStartTimeInNS = System.nanoTime();
      ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
      record.keyIndex = key.getKeyIndex();
      record.value = ByteBuffer.wrap(resultSerializer.serialize(result));
      response.getStats()
          .addReadComputeSerializationLatency(LatencyUtils.getElapsedTimeFromNSToMS(serializeStartTimeInNS));
      response.getStats().addReadComputeOutputSize(record.value.remaining());
      response.addRecord(record);
      return true;
    } else if (isStreaming) {
      // For streaming, we need to send back non-existing keys
      ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
      // Negative key index to indicate non-existing key
      record.keyIndex = Math.negateExact(key.getKeyIndex());
      record.value = StreamingUtils.EMPTY_BYTE_BUFFER;
      response.addRecord(record);
    }
    return false;
  }

  private GenericRecord computeResult(
      List<ComputeOperation> operations,
      List<Schema.Field> operationResultFields,
      PerStoreVersionState storeVersion,
      ComputeRouterRequestKeyV1 key,
      GenericRecord reusableValueRecord,
      int readerSchemaId,
      VeniceCompressor compressor,
      ReadResponseStats response,
      ReusableObjects reusableObjects,
      GenericRecord reusableResultRecord) {
    reusableValueRecord =
        readValueRecord(key, storeVersion, readerSchemaId, compressor, response, reusableObjects, reusableValueRecord);
    if (reusableValueRecord == null) {
      return null;
    }

    long computeStartTimeInNS = System.nanoTime();
    reusableResultRecord = ComputeUtils.computeResult(
        operations,
        operationResultFields,
        reusableObjects.computeContext,
        reusableValueRecord,
        reusableResultRecord);
    response.addReadComputeLatency(LatencyUtils.getElapsedTimeFromNSToMS(computeStartTimeInNS));
    return reusableResultRecord;
  }

  private GenericRecord readValueRecord(
      ComputeRouterRequestKeyV1 key,
      PerStoreVersionState storeVersion,
      int readerSchemaId,
      VeniceCompressor compressor,
      ReadResponseStats response,
      ReusableObjects reusableObjects,
      GenericRecord reusableValueRecord) {
    return GenericRecordChunkingAdapter.INSTANCE.get(
        storeVersion.storageEngine,
        key.getPartitionId(),
        ByteUtils.extractByteArray(key.getKeyBytes()),
        reusableObjects.byteBuffer,
        reusableValueRecord,
        reusableObjects.binaryDecoder,
        storeVersion.storageEngine.isChunked(),
        response,
        readerSchemaId,
        storeVersion.storeDeserializerCache,
        compressor);
  }

  private static void incrementOperatorCounters(
      ReadResponseStats response,
      Iterable<ComputeOperation> operations,
      int hits) {
    for (ComputeOperation operation: operations) {
      switch (ComputeOperationType.valueOf(operation)) {
        case DOT_PRODUCT:
          response.incrementDotProductCount(hits);
          break;
        case COSINE_SIMILARITY:
          response.incrementCosineSimilarityCount(hits);
          break;
        case HADAMARD_PRODUCT:
          response.incrementHadamardProductCount(hits);
          break;
        case COUNT:
          response.incrementCountOperatorCount(hits);
          break;
      }
    }
  }

  private AdminResponse handleServerAdminRequest(AdminRequest adminRequest) {
    switch (adminRequest.getServerAdminAction()) {
      case DUMP_INGESTION_STATE:
        String topicName = adminRequest.getStoreVersion();
        Integer partitionId = adminRequest.getPartition();
        ComplementSet<Integer> partitions =
            (partitionId == null) ? ComplementSet.universalSet() : ComplementSet.of(partitionId);
        return ingestionMetadataRetriever.getConsumptionSnapshots(topicName, partitions);
      case DUMP_SERVER_CONFIGS:
        AdminResponse configResponse = new AdminResponse();
        if (this.serverConfig == null) {
          configResponse.setError(true);
          configResponse.setMessage("Server config doesn't exist");
        } else {
          configResponse.addServerConfigs(this.serverConfig.getClusterProperties().toProperties());
        }
        return configResponse;
      default:
        throw new VeniceException("Not a valid admin action: " + adminRequest.getServerAdminAction().toString());
    }
  }

  private TopicPartitionIngestionContextResponse handleTopicPartitionIngestionContextRequest(
      TopicPartitionIngestionContextRequest topicPartitionIngestionContextRequest) {
    Integer partition = topicPartitionIngestionContextRequest.getPartition();
    String versionTopic = topicPartitionIngestionContextRequest.getVersionTopic();
    String topicName = topicPartitionIngestionContextRequest.getTopic();
    return ingestionMetadataRetriever.getTopicPartitionIngestionContext(versionTopic, topicName, partition);
  }
}

package encoder;

import adapter.AdapterType;
import adapter.ByteAdapter;
import adapter.DoubleAdapter;
import executor.Executor;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Encoder implements Executor {
  private static final String splitDelim = " |:|=";
  private static final String delim = " ";
  private static final String comment = "#";
  private static final String endl = "\n";
  private static final int sleepTime = 500;
  private static final Map<String, targetType> tMap;
  private static final Map<String, confTypes> configMap;
  private static final Map<String, tableMethodType> metMap;

  private enum targetType {
    ENCODE,
    DECODE,
    READ,
    WRITE
  }

  private enum tableMethodType {
    READ,
    WRITE
  }

  private enum confTypes {
    BLOCK_SIZE,
    SEQUENCE_LEN,
    TEXT_LEN,
    PROBABILITY,
    DECODE_CONF,
    TARGET,
    TABLE_FILE,
    TABLE_METHOD,
    START
  }

  class DoubleAdapterClass implements DoubleAdapter {
    private int index = 0, offset, block;

    @Override
    public Double getNextDouble() {
      if (block == 0)
        block = ((ArrayList<Byte>) dataOut).size();
      if (index >= ((ArrayList<Double>) dataOut).size() || index >= offset + block) {
        index = offset;
        return null;
      }
      return ((ArrayList<Double>) dataOut).get(index++);
    }

    @Override
    public void setMetrics(int start, int offset) {
      this.offset = start;
      this.block = offset;
      index = start;
    }

    @Override
    public Integer getStart() {
      return start;
    }
  }

  class ByteAdapterClass implements ByteAdapter {
    private int index = 0, offset, block;

    @Override
    public Byte getNextByte() {
      if (block == 0)
        block = ((ArrayList<Byte>) dataOut).size();
      if (index >= ((ArrayList<Byte>) dataOut).size() || index >= offset + block) {
        index = offset;
        return null;
      }
      return ((ArrayList<Byte>) dataOut).get(index++);
    }

    @Override
    public void setMetrics(int start, int offset) {
      this.offset = start;
      this.block = offset;
      index = start;
    }

    @Override
    public Integer getStart() {
      return start;
    }
  }

  static {
    tMap = new HashMap<>();
    tMap.put("encode", targetType.ENCODE);
    tMap.put("decode", targetType.DECODE);
    tMap.put("read", targetType.READ);
    tMap.put("write", targetType.WRITE);

    configMap = new HashMap<>();
    configMap.put("num", confTypes.SEQUENCE_LEN);
    configMap.put("len", confTypes.TEXT_LEN);
    configMap.put("prob", confTypes.PROBABILITY);
    configMap.put("decconf", confTypes.DECODE_CONF);
    configMap.put("target", confTypes.TARGET);
    configMap.put("block", confTypes.BLOCK_SIZE);
    configMap.put("table", confTypes.TABLE_FILE);
    configMap.put("table_method", confTypes.TABLE_METHOD);
    configMap.put("start", confTypes.START);

    metMap = new HashMap<>();
    metMap.put("read", tableMethodType.READ);
    metMap.put("write", tableMethodType.WRITE);
  }

  private DataInputStream inputFile;
  private DataOutputStream outputFile;
  private String tableFile;
  private Map<Byte, Double> probability;
  private Map<Byte, Segment> segs;
  private int textLen, numSeq, blockSize, dataLen, start;
  private ArrayList<Executor> consumers;
  private Map<Executor, Pair<Object, AdapterType>> adapters;
  private Object dataOut;
  private boolean isReadyToRead;
  private boolean isReadyToWrite;
  private boolean isAvailable;
  private boolean over;
  private targetType target = targetType.ENCODE;
  private ArrayList<AdapterType> readableTypes;
  private ArrayList<AdapterType> writableTypes;
  private Map<Integer, Pair<Object, AdapterType>> startToAdapter;
  private ArrayList<Integer> starts;

  public Encoder() {
    probability = new HashMap<>();
    readableTypes = new ArrayList<>();
    writableTypes = new ArrayList<>();
    consumers = new ArrayList<>();
    adapters = new HashMap<>();
    segs = new HashMap<>();
    textLen = 0;
    start = 0;
    over = false;
    startToAdapter = new HashMap<>();
    starts = new ArrayList<>();
  }

  private void setConfigs(String confFile) throws IOException {
    BufferedReader configReader = new BufferedReader(new FileReader(confFile));
    String line;
    while ((line = configReader.readLine()) != null) {
      String[] words = line.split(splitDelim);
      if (words.length != 2 && words.length != 3)
        throw new IOException("Wrong number of arguments in file: " + confFile + " at: " + line);
      if (words[0].startsWith(comment))
        continue;
      confTypes type = configMap.get(words[0]);
      if (type == null)
        throw new IOException("Unknown config: " + words[0] + " in file: " + confFile + " at: " + line);
      switch (type) {
        case SEQUENCE_LEN: {
          numSeq = Integer.parseInt(words[1]);
          break;
        }
        case START: {
          start = Integer.parseInt(words[1]);
          break;
        }
        case TEXT_LEN: {
          textLen = Integer.parseInt(words[1]);
          break;
        }
        case PROBABILITY: {
          byte ch = (byte) Integer.parseInt(words[1]);
          probability.put(ch, Double.parseDouble(words[2]));
          segs.put(ch, new Segment());
          break;
        }
        case TARGET: {
          target = tMap.get(words[1]);
          if (target == null)
            throw new IOException("Unknown target: " + words[1] + " in file: " + confFile + " at: " + line + " decode|encode expected");
          switch (target) {
            case ENCODE: {
              isReadyToRead = true;
              isReadyToWrite = false;
              isAvailable = true;
              readableTypes.add(AdapterType.BYTE);
              writableTypes.add(AdapterType.DOUBLE);
              break;
            }
            case DECODE: {
              isReadyToRead = true;
              isReadyToWrite = false;
              isAvailable = true;
              writableTypes.add(AdapterType.BYTE);
              readableTypes.add(AdapterType.DOUBLE);
              break;
            }
            case READ: {
              isReadyToRead = false;
              isReadyToWrite = false;
              isAvailable = false;
              writableTypes.add(AdapterType.BYTE);
              break;
            }
            case WRITE: {
              isReadyToRead = true;
              isReadyToWrite = false;
              isAvailable = true;
              readableTypes.add(AdapterType.BYTE);
              readableTypes.add(AdapterType.DOUBLE);
              break;
            }
          }
          break;
        }
        case BLOCK_SIZE: {
          blockSize = Integer.parseInt(words[1]);
          break;
        }
        case TABLE_FILE: {
          tableFile = words[1];
          break;
        }
        case TABLE_METHOD: {
          tableMethodType tm = metMap.get(words[1]);
          if (tm == null)
            throw new IOException("Unknown method: " + words[1] + "in file: " + confFile + " at: " + line + " read|write expected");
          switch (tm) {
            case READ: {
              setConfigs(tableFile);
              break;
            }
            case WRITE: {
              if (words.length != 3)
                throw new IOException("Need file name to count probabilities");
              countProb(words[2]);
              writeDecodeConf();
            }
          }
          break;
        }
      }
    }
    configReader.close();
  }

  private void countProb(String inputFileName) throws IOException {
    DataInputStream copy = new DataInputStream(new FileInputStream(inputFileName));
    while (copy.available() > 0) {
      byte ch = copy.readByte();
      textLen++;
      if (!probability.containsKey(ch))
        probability.put(ch, 1.0);
      else
        probability.replace(ch, probability.get(ch) + 1);

      segs.putIfAbsent(ch, new Segment());
    }

    copy.close();

    for (Byte key : probability.keySet())
      probability.replace(key, probability.get(key) / textLen);

  }

  private void defineSegments() {
    double l = 0;

    for (Map.Entry<Byte, Segment> entry : segs.entrySet()) {
      entry.getValue().left = l;
      entry.getValue().right = l + probability.get(entry.getKey());
      l = entry.getValue().right;
    }
  }

  private void writeDecodeConf() throws IOException {
    BufferedWriter encWriter = new BufferedWriter(new FileWriter(tableFile));

    for (Map.Entry<String, confTypes> entry : configMap.entrySet()) {
      switch (entry.getValue()) {
        case PROBABILITY: {
          for (Map.Entry<Byte, Double> prEntry : probability.entrySet()) {
            encWriter.write(entry.getKey() + delim + prEntry.getKey() + delim + prEntry.getValue() + endl);
          }
          break;
        }
      }
    }
    encWriter.close();
  }

  public Object code(Object data) {
    switch (target) {
      case ENCODE: {
        try {
          return encode((ArrayList<Byte>) data);
        } catch (IOException ex) {


          System.exit(1);
        }
        break;
      }
      case DECODE: {
        try {
          return decode((ArrayList<Double>) data);
        } catch (IOException ex) {
          System.out.println(ex.getMessage());
          System.exit(1);
        }
        break;
      }
    }
    return null;
  }

  private ArrayList<Double> encode(ArrayList<Byte> data) throws IOException {

    defineSegments();

    int size = (int) Math.ceil((double) data.size() / numSeq);
    ArrayList<Double> newData = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      double left = 0, right = 1;
      for (int j = 0; j < numSeq; j++) {
        if (i * numSeq + j >= dataLen)
          break;
        byte ch = data.get(i * numSeq + j);
        double newR = left + (right - left) * segs.get(ch).right;
        double newL = left + (right - left) * segs.get(ch).left;
        right = newR;
        left = newL;
      }
      newData.add((left + right) / 2);
    }

    return newData;
  }

  private ArrayList<Byte> decode(ArrayList<Double> data) throws IOException {

    defineSegments();

    ArrayList<Byte> newData = new ArrayList<>(numSeq * data.size());

    for (int i = 0; i < data.size(); i++) {
      double code = data.get(i);
      for (int j = 0; j < numSeq; j++) {
        for (Map.Entry<Byte, Segment> entry : segs.entrySet())
          if (code >= entry.getValue().left && code < entry.getValue().right) {
            newData.add(numSeq * i + j, entry.getKey());
            code = (code - entry.getValue().left) / (entry.getValue().right - entry.getValue().left);
            break;
          }
      }
    }

    return newData;
  }

  private boolean consumersReadyToRead() {
    for (Executor cons : consumers) {
      if (!cons.isAvailable() || !cons.isReadyToRead())
          return false;
    }
    return true;
  }

  private boolean providersReadyToWrite() {
    for (Executor prov : adapters.keySet()) {
      if (!prov.isAvailable() || !prov.isReadyToWrite())
          return false;
    }
    return true;
  }

  @Override
  public void setConfigFile(String configFile) throws IOException {
    setConfigs(configFile);
  }

  @Override
  public void setConsumer(Executor consumer) throws IOException {
    consumers.add(consumer);
    boolean canCommunicate = false;

    for (adapter.AdapterType type : consumer.getReadableTypes()) {
      if (writableTypes.contains(type)) {
        canCommunicate = true;
        switch (type) {
          case BYTE: {
            consumer.setAdapter(this, new ByteAdapterClass(), AdapterType.BYTE);
            break;
          }
          case DOUBLE: {
            consumer.setAdapter(this, new DoubleAdapterClass(), AdapterType.DOUBLE);
            break;
          }
        }
      }
    }

    if (!canCommunicate) {
      throw new IOException("Can't communicate, wrong transporter structure");
    }
  }

  @Override
  public void setAdapter(Executor provider, Object adapter, AdapterType typeOfAdapter) {
    switch (typeOfAdapter) {
      case DOUBLE: {
        ((DoubleAdapter) adapter).setMetrics(start, blockSize);
        break;
      }
      case BYTE: {
        ((ByteAdapter) adapter).setMetrics(start, blockSize);
        break;
      }
    }
    adapters.put(provider, new Pair<>(adapter, typeOfAdapter));
  }

  @Override
  public ArrayList<AdapterType> getReadableTypes() {
    return readableTypes;
  }

  @Override
  public void setOutput(DataOutputStream output) {
    outputFile = output;
  }

  @Override
  public void setInput(DataInputStream input) {
    inputFile = input;
  }

  @Override
  public boolean isReadyToWrite() {
    return isReadyToWrite;
  }

  @Override
  public boolean isReadyToRead() {
    return isReadyToRead;
  }

  @Override
  public boolean isAvailable() {
    return isAvailable;
  }

  @Override
  public boolean isOver() {
    return over;
  }

  private void runReader() {
    try {
      while (!Thread.interrupted()) {
        while (inputFile.available() > 0) {
          if (!consumersReadyToRead())
            continue;
          isReadyToWrite = false;
          isAvailable = false;
          byte[] data = new byte[blockSize];
          if (inputFile.available() > blockSize)
            dataLen = blockSize;
          else
            dataLen = inputFile.available();
          int readLen = inputFile.read(data, 0, dataLen);
          ArrayList<Byte> bdata = new ArrayList<>();
          for (int i = 0; i < readLen; i++)
            bdata.add(data[i]);
          dataOut = bdata;
          isReadyToWrite = true;
          isAvailable = true;

          Thread.sleep(sleepTime);
        }
        if (!consumersReadyToRead())
          continue;
        isReadyToWrite = false;
        isAvailable = false;
        over = true;
      }
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    } catch (InterruptedException ex) {
    }
  }

  private void runWriter() {
    sortAdapters();
    try {
      while (!Thread.interrupted()) {
        if (!providersReadyToWrite())
          continue;
        isReadyToRead = false;
        isAvailable = false;
        for (Integer st : starts) {
          Pair<Object, AdapterType> ad = startToAdapter.get(st);
          switch (ad.getValue()) {
            case DOUBLE: {
              DoubleAdapter doubleAdapter = (DoubleAdapter) ad.getKey();
              Double cur;
              while ((cur = doubleAdapter.getNextDouble()) != null) {
                outputFile.writeDouble(cur);
              }
              break;
            }
            case BYTE: {
              ByteAdapter byteAdapter = (ByteAdapter) ad.getKey();
              Byte cur;
              while ((cur = byteAdapter.getNextByte()) != null) {
                outputFile.write(cur);
              }
              break;
            }
          }
        }
        isReadyToRead = true;
        isAvailable = true;

        Thread.sleep(sleepTime);
      }
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    } catch (InterruptedException ex) {
    }
  }

  private void runCoder() {
    sortAdapters();
    try {
      while (!Thread.interrupted()) {
        if (!providersReadyToWrite())
          continue;
        if (!consumersReadyToRead()) {
          isReadyToRead = false;
          continue;
        }
        for (Integer st : starts) {
          Pair<Object, AdapterType> ad = startToAdapter.get(st);
          isReadyToRead = false;
          isReadyToWrite = false;
          isAvailable = false;

          Object currentAdapter = ad.getKey();
          AdapterType type = ad.getValue();

          switch (type) {
            case BYTE: {
              ArrayList<Byte> bdata = new ArrayList<>();
              ByteAdapter byteAdapter = (ByteAdapter) currentAdapter;
              Byte cur;
              while ((cur = byteAdapter.getNextByte()) != null) {
                bdata.add(cur);
              }
              dataLen = bdata.size();
              dataOut = code(bdata);
              break;
            }
            case DOUBLE: {
              ArrayList<Double> ddata = new ArrayList<>();
              DoubleAdapter doubleAdapter = (DoubleAdapter) currentAdapter;
              Double cur;
              while ((cur = doubleAdapter.getNextDouble()) != null) {
                ddata.add(cur);
              }
              dataLen = ddata.size();
              dataOut = code(ddata);
              break;
            }
          }
          isAvailable = true;
          isReadyToWrite = true;

          Thread.sleep(sleepTime);
        }
        while (!consumersReadyToRead())
          ;
        isReadyToRead = true;
        isReadyToWrite = false;
        isAvailable = true;

        Thread.sleep(sleepTime);
      }
    } catch (InterruptedException ex) {
    }
  }

  private void sortAdapters() {
    for (Pair<Object, AdapterType> ad : adapters.values()) {
      switch (ad.getValue()) {
        case DOUBLE: {
          Integer st = ((DoubleAdapter)ad.getKey()).getStart();
          starts.add(st);
          startToAdapter.put(st, ad);
          break;
        }
        case BYTE: {
          Integer st = ((ByteAdapter)ad.getKey()).getStart();
          starts.add(st);
          startToAdapter.put(st, ad);
          break;
        }
      }
    }
    Collections.sort(starts);
  }

  @Override
  public void run() {
    switch (target) {
      case READ: {
        runReader();
        break;
      }
      case WRITE: {
        runWriter();
        break;
      }
      default: {
        runCoder();
        break;
      }
    }
  }
}
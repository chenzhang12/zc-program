package zc.logAnalyse;

import java.awt.BasicStroke;
import java.awt.Color;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.labels.StandardXYItemLabelGenerator;
import org.jfree.chart.labels.XYItemLabelGenerator;
import org.jfree.chart.labels.XYToolTipGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleEdge;
import org.jfree.ui.RectangleInsets;


/**
 * An example of a time series chart. For the most part, default settings are
 * used, except that the renderer is modified to show filled shapes (as well as
 * lines) at each data point.
 * <p>
 * IMPORTANT NOTE: THIS DEMO IS DOCUMENTED IN THE JFREECHART DEVELOPER GUIDE. DO
 * NOT MAKE CHANGES WITHOUT UPDATING THE GUIDE ALSO!!
 */
//extends ApplicationFrame
public class AppGraphics {
	
	private DataInputStream din = null;
	private String taskAttemptId = null;
	//private int len = -1;
	private String unit = null;
	//TimeSeries mapCompleteEventMarks = new TimeSeries("Map Complete Event and Copy phase complete marks", Millisecond.class);
	//TimeSeries sortPhaseStartMarks = new TimeSeries("sort phase start marks",Millisecond.class);
	//TimeSeries finalMergeStartMarks = new TimeSeries("final merge start marks", Millisecond.class);
	//TimeSeries sortPhaseCompleteMarks = new TimeSeries("sort phase complete marks", Millisecond.class);
	
	final double gmarkCeiling = 1024; //where to start the gmark
	final double gmarkStepSize = 5; // For each map completion event, the line get down gmarkStepSize.
																		// If there are too many maps. Turn this value down properly.
	public static enum CurveType {
		REAL,
		USED,
		ALLOC,
		ESTIMATED
	}
	
	static CurveType curveToDraw = CurveType.REAL;
	
	public static void setCurveTypeToDraw(CurveType curveTypeToDraw) {
		curveToDraw = curveTypeToDraw;
	}
	
	public String getTaskAttemptId() {
		return taskAttemptId;
	}
	/**
	 * A demonstration application showing how to create a simple time series
	 * chart. This example uses monthly data.
	 * 
	 * @param title
	 *            the frame title.
	 * @throws IOException 
	 */
	public AppGraphics(String title, DataInputStream taskFileInputStream) {
		// super(title);
		din = taskFileInputStream;
		try {
			taskAttemptId = din.readUTF();
			//len = din.readInt(); // record number
			unit = din.readUTF();
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
	}
	
	public static void display(String title, DataInputStream taskFileInputStream, String chartOutDir) {
		AppGraphics demo = new AppGraphics(title,taskFileInputStream);

		XYDataset dataset = null;
		try {
			dataset = demo.createDataset();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		JFreeChart chart;
		try {
			chart = demo.createChart(dataset);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		FileOutputStream chartOut = null;
		try {
			chartOut = new FileOutputStream(chartOutDir + "/" +demo.getTaskAttemptId());
			ChartUtilities.writeChartAsPNG(chartOut, chart, 600, 300);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(chartOut != null) chartOut.close();
			} catch (IOException e) {}
		}
		//ChartPanel chartPanel = new ChartPanel(chart);
		//chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
		//chartPanel.setMouseZoomable(true, false);
		//demo.setContentPane(chartPanel);
		//demo.pack();
		//RefineryUtilities.centerFrameOnScreen(demo);
		//demo.setVisible(true);
	}

	/**
	 * Creates a chart.
	 * 
	 * @param dataset
	 *            a dataset.
	 * 
	 * @return A chart.
	 * @throws IOException 
	 */
	private JFreeChart createChart(XYDataset dataset) throws IOException {
		JFreeChart chart = ChartFactory.createTimeSeriesChart(
				taskAttemptId, // title
				"time", // x-axis label
				"Memory Usage" + "("+unit+")", // y-axis label
				dataset, // data
				true, // create legend?
				true, // generate tooltips?
				false // generate URLs?
				);
		chart.setBackgroundPaint(Color.white);
		chart.getLegend().setBorder(0, 0, 0, 0);
		chart.getLegend().setPosition(RectangleEdge.TOP);
		XYPlot plot = (XYPlot) chart.getPlot();
		plot.setBackgroundPaint(Color.white);
		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.white);
		plot.setAxisOffset(new RectangleInsets(0, 0, 0, 0));
		plot.setDomainCrosshairVisible(true);
		plot.setRangeCrosshairVisible(true);
		//XYItemRenderer r = plot.getRenderer();
		//r.setSeriesStroke(0, new BasicStroke(3F));
		//if (r instanceof XYLineAndShapeRenderer) {
			//XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
			//renderer.setBaseShapesVisible(true);
			//renderer.setBaseShapesFilled(true);
			//DecimalFormat f1=new DecimalFormat("#############.###"); 
			//renderer.setBaseItemLabelGenerator(new StandardXYItemLabelGenerator("{2}",f1,f1));
			//renderer.setBaseItemLabelsVisible(true);
		//}
		DateAxis axis = (DateAxis) plot.getDomainAxis();
		axis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss,SSS"));
		return chart;
	}

	/**
	 * Creates a dataset, consisting of two series of monthly data.
	 * 
	 * @return the dataset.
	 * @throws IOException 
	 */
	
	private XYDataset createDataset() throws IOException {
		
		List<TimeSeries> curves = new ArrayList<>();
		//TimeSeries s3 = new TimeSeries("Memory used within jvm",Millisecond.class);
		//TimeSeries s4 = new TimeSeries("Memory total within jvm", Millisecond.class);
		
		if(curveToDraw == CurveType.ESTIMATED) {
			int sampleNum = din.readInt();
			for(int i=0; i<sampleNum; ++i) {
				int pointsNum = din.readInt();
				TimeSeries ts = new TimeSeries("Estimated Memory of sample curve " + (i+1));
				curves.add(ts);
				for(int j=0; j<pointsNum; ++j) {
					long time = din.readLong();
					double estimatedMem = din.readDouble();
					ts.addOrUpdate(new Millisecond(new Date(time)), estimatedMem);
				}
			}
		}
		
		if(curveToDraw == CurveType.USED) {
			TimeSeries s1 = new TimeSeries("Memory Usage of the task in the container", Millisecond.class);
			curves.add(s1);
			int len = din.readInt();
			for(int i=0; i<len; ++i) {
				long time = din.readLong();
				double memUsed = din.readDouble();
				din.readDouble();
				s1.addOrUpdate(new Millisecond(new Date(time)), memUsed);
			}
		}
		
		if(curveToDraw == CurveType.ALLOC) {
			TimeSeries s2 = new TimeSeries("Memory Alloc for the container", Millisecond.class);
			curves.add(s2);
			int len = din.readInt();
			for(int i=0; i<len; ++i) {
				long time = din.readLong();
				din.readDouble();
				double memTotal = din.readDouble();
				s2.addOrUpdate(new Millisecond(new Date(time)), memTotal);
			}
		}
		
		if(curveToDraw == CurveType.REAL) {
			TimeSeries s1 = new TimeSeries("Memory Usage of the task in the container", Millisecond.class);
			TimeSeries s2 = new TimeSeries("Memory Alloc for the container", Millisecond.class);
			curves.add(s1);
			curves.add(s2);
			int len = din.readInt();
			for(int i=0; i<len; ++i) {
				long time = din.readLong();
				double memUsed = din.readDouble();
				double memTotal = din.readDouble();
				s1.addOrUpdate(new Millisecond(new Date(time)), memUsed);
				s2.addOrUpdate(new Millisecond(new Date(time)), memTotal);
			}
		}
		
		/*
		int len2 = din.readInt();
		for(int i=0; i<len2; ++i) {
			long time = din.readLong();
			double memUsed = din.readDouble();
			double memTotal = din.readDouble();
			s3.addOrUpdate(new Millisecond(new Date(time)), memUsed);
			s4.addOrUpdate(new Millisecond(new Date(time)), memTotal);
		}
		// read marks to mark the timeseries
		// boolean isFirstGmark = true;
		double lastTopMarkValue = this.gmarkCeiling;
		int len3 = din.readInt();
		for(int i=0; i<len3; ++i) {
			long time = din.readLong();
			String type = din.readUTF();
			int completeMaps = din.readInt();
			if(type.equals(GMark.mapComplete)) {
				lastTopMarkValue -= completeMaps * this.gmarkStepSize;
				addGmark(mapCompleteEventMarks, time, lastTopMarkValue);
			} else if (type.equals(GMark.copyPhaseEnd)) {
				addGmark(mapCompleteEventMarks, time, 0);
				this.formerValue = this.gmarkCeiling;
			} else if (type.equals(GMark.sortPhaseStart)) {
				addGmark(sortPhaseStartMarks, time, 0);
				this.formerValue = this.gmarkCeiling;
			} else if (type.equals(GMark.finalMergeStart)) {
				addGmark(finalMergeStartMarks, time, 0);
				this.formerValue = this.gmarkCeiling;
			} else if (type.equals(GMark.sortPhaseEnd)) {
				addGmark(sortPhaseCompleteMarks, time, 0);
				this.formerValue = this.gmarkCeiling;
			}
		}
		*/
		din.close();
		TimeSeriesCollection dataset = new TimeSeriesCollection();
		for(TimeSeries ts : curves) {
			dataset.addSeries(ts); 
		}
		//dataset.addSeries(s3);
		//dataset.addSeries(s4);
		//dataset.addSeries(mapCompleteEventMarks);
		//dataset.addSeries(sortPhaseStartMarks);
		//dataset.addSeries(finalMergeStartMarks);
		//dataset.addSeries(sortPhaseCompleteMarks);
		dataset.setDomainIsPointsInTime(true);
		return dataset;
	}
	
	double formerValue = this.gmarkCeiling;
	private void addGmark(TimeSeries gmarkSeries, long timeMilli, double markValue) {	
		addmrk(gmarkSeries, timeMilli, formerValue);
		addmrk(gmarkSeries, timeMilli+1, markValue);
		formerValue = markValue;
	}
	
	private void addmrk(TimeSeries gmarkSeries, long timeMilli, double markValue) {
		gmarkSeries.addOrUpdate(new Millisecond(new Date(timeMilli)), markValue);
	}
	/**
	 * Creates a panel for the demo (used by SuperDemo.java).
	 * 
	 * @return A panel.
	 */
	/*
	public JPanel createDemoPanel() {
		JFreeChart chart = createChart(createDataset());
		return new ChartPanel(chart);
	}
	*/

	/**
	 * Starting point for the demonstration application.
	 * 
	 * @param args
	 *            ignored.
	 */
	public static void main(String[] args) {
		if(args.length < 9) {
			System.out.println("Usage: -logBase <memUsageLogDir> -appattemptId <appattemptId> -displayNum <mapNum> <reduceNum> -outputDir <appChartsOutputDir>");
			System.exit(1);
		}
		String logBase = null;
		String appid = null;
		String appChartsOutDir = null;
		int mapNum = 0;
		int reduceNum = 0;
		
		if(args[0].equals("-logBase")) {
			logBase = args[1];
		} else {
			System.out.println("argument invalid, please input the correct log base dir");
			System.exit(1);
		}
		if(args[2].equals("-appattemptId")) {
			appid = args[3];
		} else {
			System.out.println("argument invalid, it should be \"-appattemptId\".");
		}
		if(args[4].equals("-displayNum")) {
			mapNum = Integer.parseInt(args[5]);
			reduceNum = Integer.parseInt(args[6]);
		} else {
			System.out.println("argument invalid, it should be \"-displayNum\".");
		}
		if(args[7].equals("-outputDir")) {
			appChartsOutDir= args[8];
		} else {
			System.out.println("argument invalid, it should be \"-displayNum\".");
		}
		System.out.println(mapNum + "," + reduceNum);
		
		LogFileAndGraphAdapter l = new LogFileAndGraphAdapter(logBase, appid, "map");
		DataInputStream din = null;
		AppGraphics.setCurveTypeToDraw(CurveType.REAL); // ZC: control what curve to Draw
		while (mapNum > 0 && l.hasLogFile() && (din = l.nextTaskFileInputStream()) != null) {
			AppGraphics.display("Memory Usage", din, appChartsOutDir);
			mapNum --;
		}
		
		LogFileAndGraphAdapter l2 = new LogFileAndGraphAdapter(logBase, appid, "reduce");
		DataInputStream din2 = null;
		while(reduceNum >0 && l.hasLogFile() && (din2 = l2.nextTaskFileInputStream()) != null) {
			AppGraphics.display("Memory Usage", din2, appChartsOutDir);
			reduceNum --;
		}		
	}
}

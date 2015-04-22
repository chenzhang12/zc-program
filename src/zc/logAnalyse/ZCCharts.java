package zc.logAnalyse;

import java.awt.BasicStroke;
import java.awt.Color;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RectangleEdge;
import org.jfree.ui.RectangleInsets;

import zc.logAnalyse.ZCDatasets.AppAttemptData;
import zc.logAnalyse.ZCDatasets.MetricType;
import zc.logAnalyse.ZCDatasets.ParaRanges;
import zc.logAnalyse.ZCDatasets.ParameterType;
import zc.logAnalyse.ZCDatasets.WorkLoadType;
import static zc.logAnalyse.ZCDatasets.ParameterType.*;
import static zc.logAnalyse.ZCDatasets.MetricType.*;

public class ZCCharts {
	/**
	 * 
	 * @param title
	 *          图表标题
	 * @param categoryAxisLabel
	 *          目录轴的显示标签
	 * @param valueAxisLabel
	 *          数值轴的显示标签
	 * @param dataset
	 *          数据集
	 * @param legend
	 *          是否显示图例(对于简单的柱状图必须是 false)
	 * @return
	 * @throws IOException
	 */
	private static JFreeChart createBarChart(String title,
			String categoryAxisLabel, String valueAxisLabel, CategoryDataset dataset,
			boolean legend) {
		JFreeChart chart = ChartFactory.createBarChart(title, // 图表标题
				categoryAxisLabel, // 目录轴的显示标签
				valueAxisLabel, // 数值轴的显示标签
				dataset, // 数据集
				PlotOrientation.VERTICAL, // 图表方向：水平、垂直
				true, // 是否显示图例(对于简单的柱状图必须是 false)
				false, // 是否生成工具
				false // 是否生成 URL 链接
				);
		chart.setBackgroundPaint(Color.white);
		if(legend) {
			chart.getLegend().setBorder(0, 0, 0, 0);
			chart.getLegend().setPosition(RectangleEdge.TOP);
		}
		CategoryPlot plot = (CategoryPlot) chart.getPlot();
		plot.setBackgroundPaint(Color.white);
		plot.setOutlineVisible(false);
		plot.setAxisOffset(new RectangleInsets(0, 0, 0, 0));
		CategoryPlot cplot = (CategoryPlot) chart.getCategoryPlot();
		BarRenderer barRenderer = (BarRenderer) cplot.getRenderer();
		barRenderer.setBarPainter(new StandardBarPainter());
		barRenderer.setItemMargin(0.0);
		// plot.setDomainGridlinePaint(Color.gray);
		plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
		return chart;
	}
	
	private static JFreeChart createStackedBarChart(String title,
			String categoryAxisLabel, String valueAxisLabel, CategoryDataset dataset,
			boolean legend) {
		JFreeChart chart = ChartFactory.createStackedBarChart(title, // 图表标题
				categoryAxisLabel, // 目录轴的显示标签
				valueAxisLabel, // 数值轴的显示标签
				dataset, // 数据集
				PlotOrientation.HORIZONTAL, // 图表方向：水平、垂直
				true, // 是否显示图例(对于简单的柱状图必须是 false)
				false, // 是否生成工具
				false // 是否生成 URL 链接
				);
		chart.setBackgroundPaint(Color.white);
		if(legend) {
			chart.getLegend().setBorder(0, 0, 0, 0);
			chart.getLegend().setPosition(RectangleEdge.TOP);
		}
		CategoryPlot plot = (CategoryPlot) chart.getPlot();
		plot.setBackgroundPaint(Color.white);
		plot.setOutlineVisible(false);
		plot.setAxisOffset(new RectangleInsets(0, 0, 0, 0));
		CategoryPlot cplot = (CategoryPlot) chart.getCategoryPlot();
		BarRenderer barRenderer = (BarRenderer) cplot.getRenderer();
		barRenderer.setBarPainter(new StandardBarPainter());
		barRenderer.setItemMargin(0.0);
		// plot.setDomainGridlinePaint(Color.gray);
		plot.setRangeGridlinePaint(Color.LIGHT_GRAY);
		return chart;
	}
	
	private static JFreeChart createXYLineChart(String title, String xAxisLabel, String yAxisLabel, XYDataset dataset, boolean legend) {
		JFreeChart chart = ChartFactory.createXYLineChart(title, xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, legend, false, false);
		chart.setBackgroundPaint(Color.white);
		if(legend) {
			chart.getLegend().setBorder(0, 0, 0, 0);
			chart.getLegend().setPosition(RectangleEdge.TOP);
		}
		XYPlot plot = (XYPlot) chart.getPlot();
		plot.setBackgroundPaint(Color.lightGray);
		plot.setOutlineVisible(false);
		plot.setAxisOffset(new RectangleInsets(0, 0, 0, 0));
		int size = dataset.getSeriesCount();
		for(int i=0;i<size;++i) {
			XYItemRenderer rd = plot.getRenderer();
			rd.setSeriesStroke(i, new BasicStroke(2.0f));
		}
		plot.setDomainGridlinePaint(Color.lightGray);
		plot.setRangeGridlinePaint(Color.gray);
		return chart;
	}

	public static void displayBarChart(OutputStream out, CategoryDataset dataset,
			String title, String categoryAxisLabel, String valueAxisLabel,
			boolean legend) {
		try {
			JFreeChart chart = createBarChart(title, categoryAxisLabel,
					valueAxisLabel, dataset, legend);
			ChartUtilities.writeChartAsJPEG(out, 1.0f, chart, 400, 300);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void displayStackedBarChart(OutputStream out, CategoryDataset dataset,
			String title, String categoryAxisLabel, String valueAxisLabel,
			boolean legend) {
		try {
			JFreeChart chart = createStackedBarChart(title, categoryAxisLabel,
					valueAxisLabel, dataset, legend);
			ChartUtilities.writeChartAsJPEG(out, 1.0f, chart, 2000, 2000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeChartData(OutputStream out, CategoryDataset dataset) {
		String line = "";
		int rows = dataset.getRowCount();
		int cols = dataset.getColumnCount();
		// write column keys
		for(int c=0; c<cols; ++c) {
			line += "	" + dataset.getColumnKey(c);
		}
		line += "\n";
		byte[] lineBytes = line.getBytes();
		try {
			out.write(lineBytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// write lines
		line = "";
		lineBytes = null;
		for(int r=0; r<rows; ++r) {
			// row key
			line = dataset.getRowKey(r).toString();
			for(int c=0; c<cols; ++c) {
				line += "	" + dataset.getValue(r, c);
			}
			line += "\n";
			lineBytes = line.getBytes();
			try {
				out.write(lineBytes);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void writeChartData(OutputStream out, XYSeriesCollection dataset) {
		String head = "";
		boolean headWriten = false;
		int seriesNum = dataset.getSeriesCount();
		for(int i=0; i<seriesNum; ++i) {
			XYSeries series = dataset.getSeries(i);
			int itemNum = series.getItemCount();
			if(!headWriten) { // write head
				for(int j=0; j<itemNum; ++j) {
					float headNum = ((Double)series.getX(j)).floatValue();
					head += "	" + headNum;
				}
				head += "\n";
				headWriten = true;
				try {
					out.write(head.getBytes());
				} catch (IOException e) {e.printStackTrace();}
			}
			// write content
			String line = series.getKey().toString();
			for(int j=0; j<itemNum; ++j) {
				double val = (Double)series.getY(j);
				line += "	" + val;
			}
			line += "\n";
			try {
				out.write(line.getBytes());
			} catch (IOException e) {e.printStackTrace();} 
		}
	}
	
	public static void displayLineChart(OutputStream out, XYDataset dataset, String title, String xAxisLabel, String yAxisLabel, boolean legend) {
		try {
			JFreeChart chart = createXYLineChart(title, xAxisLabel,
					yAxisLabel, dataset, legend);
			ChartUtilities.writeChartAsPNG(out, chart, 400, 300);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void displayEvaOfPredra(ZCDatasets dataset, String outputDir, WorkLoadType wt) {
		FileOutputStream fout = null;
		String targetDir = null;
		File tdir = null;
		try {
			ParameterType[] paraTypes = new ParameterType[] {Delta, Beta, Gamma, Sigma};
			MetricType[] metricTypes = new MetricType[] {TP, AJTT, AJWT, AP, MUR, PFR, PPREM};
			targetDir = outputDir + "/" + wt +"/predra";
			tdir = new File(targetDir);
			if(!tdir.exists()) tdir.mkdirs();
			
			// output abs charts
			for(ParameterType pt : paraTypes) {
				for(MetricType mt : metricTypes) {
					fout = new FileOutputStream(targetDir +"/" + pt.toString() + "_" + mt.toString() + "_abs");
					displayLineChart(fout, dataset.getPredraAbsDataset(wt, pt, mt, true),
							null, pt.toString(), mt.toString(), true);
					fout.close();
				}
			}
			
			// output comp charts
			for(ParameterType pt : paraTypes) {
				for(MetricType mt : metricTypes) {
					if(mt == MetricType.PFR || mt == MetricType.PPREM) continue;
					fout = new FileOutputStream(targetDir +"/" + pt.toString() + "_" + mt.toString()+ "_comp");
					displayLineChart(fout, dataset.getPredraCompDataset(wt, pt, mt, true),
							null, pt.toString(), mt.toString(), true);
					fout.close();
				}
			}
			
			// output Comp data of charts
			for(ParameterType pt : paraTypes) {
				fout = new FileOutputStream(targetDir +"/" + pt.toString() + "_dataComp.txt");
				writeChartData(fout, dataset.getCollectedPredraCompDataset(wt, pt, metricTypes, false));
				fout.close();
			}
			
			// output Abs data of charts
			for(ParameterType pt : paraTypes) {
				fout = new FileOutputStream(targetDir +"/" + pt.toString() + "_data.txt");
				writeChartData(fout, dataset.getCollectedPredraAbsDataset(wt, pt, metricTypes, false));
				fout.close();
			}
			/*
			// output delta
			fout = new FileOutputStream(targetDir +"/delta");
			displayLineChart(fout, dataset.getNormalizedSoloDataset(wt, ParameterType.Delta),
					null, "delta", "normalized indicators", true);
			fout.close();
			// output beta
			fout = new FileOutputStream(targetDir +"/beta");
			displayLineChart(fout, dataset.getNormalizedSoloDataset(wt, ParameterType.Beta),
					null, "beta", "normalized indicators", true);
			fout.close();
			// output gamma
			fout = new FileOutputStream(targetDir +"/gamma");
			displayLineChart(fout, dataset.getNormalizedSoloDataset(wt, ParameterType.Gamma),
					null, "gamma", "normalized indicators", true);
			fout.close();
			// output sigma
			fout = new FileOutputStream(targetDir +"/sigma");
			displayLineChart(fout, dataset.getNormalizedSoloDataset(wt, ParameterType.Sigma),
					null, "sigma", "normalized indicators", true);
			fout.close();
			*/
		} catch (IOException e) {
			e.printStackTrace();
			if(fout != null)
				try {
					fout.close();
				} catch (IOException e1) {}
		}
	}
	
	public static void displayCompEva(ZCDatasets dataset, String outputDir, WorkLoadType wt) {
		FileOutputStream fout = null;
		String targetDir = null;
		File tdir = null;
		ParameterType[] paraTypes = new ParameterType[] {ReducesGroup, MRReq};
		MetricType[] metricTypes = new MetricType[] {TP, AJWT, AJTT, AP, MUR};
		try {
			// draw charts
			for(ParameterType pt : paraTypes) {
				targetDir = outputDir + "/" + wt + "/comp/" + pt.toString();
				tdir = new File(targetDir);
				if(!tdir.exists()) tdir.mkdirs();
				for(MetricType mt : metricTypes) {
					fout = new FileOutputStream(targetDir +"/" + mt.toString());
					displayBarChart(fout, dataset.getMRRGDataset(wt, pt, mt, true), null, pt.toString(), mt.toString(), true);
					fout.close();
				}
			}
			
			// output data of charts
			for(ParameterType pt : paraTypes) {
				targetDir = outputDir + "/" + wt + "/comp/" + pt.toString();
				tdir = new File(targetDir);
				if(!tdir.exists()) tdir.mkdirs();
				for(MetricType mt : metricTypes) {
					fout = new FileOutputStream(targetDir +"/" + mt.toString() + "_data.txt");
					writeChartData(fout, dataset.getMRRGDataset(wt, pt, mt, false));
					fout.close();
				}
			}
			
			// output comp data of charts
			for(ParameterType pt : paraTypes) {
				targetDir = outputDir + "/" + wt + "/comp/" + pt.toString();
				tdir = new File(targetDir);
				if(!tdir.exists()) tdir.mkdirs();
				for(MetricType mt : metricTypes) {
					fout = new FileOutputStream(targetDir +"/" + mt.toString() + "_dataComp.txt");
					writeChartData(fout, dataset.getMRRGCompDataset(wt, pt, mt, false));
					fout.close();
				}
			}
			
			/*
			// reduce number changes
			targetDir = outputDir + "/" + wt + "/comp/reduceNumberChanging";
			tdir = new File(targetDir);
			if(!tdir.exists()) tdir.mkdirs();
				// TP(Per Hour)
			fout = new FileOutputStream(targetDir +"/TP");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.ReducesGroup, 
					MetricType.TP), null, "Reduce Group", "TP", true);
			fout.close();
			  // AJWT
			fout = new FileOutputStream(targetDir +"/AJWT");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.ReducesGroup, 
					MetricType.AJWT), null, "Reduce Group", "AJWT", true);
			fout.close();
			  // AJTT
			fout = new FileOutputStream(targetDir +"/AJTT");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.ReducesGroup, 
					MetricType.AJTT), null, "Reduce Group", "AJTT", true);
			fout.close();
			  // AP
			fout = new FileOutputStream(targetDir +"/AP");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.ReducesGroup, 
					MetricType.AP), null, "Reduce Group", "AP", true);
			fout.close();
			  // MUR
			fout = new FileOutputStream(targetDir +"/MUR");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.ReducesGroup, 
					MetricType.MUR), null, "Reduce Group", "MUR", true);
			fout.close();
			
			// mr request changes
			targetDir = outputDir + "/" + wt + "/comp/mrRequestChanging";
			tdir = new File(targetDir);
			if(!tdir.exists()) tdir.mkdirs();
				// TP(Per Hour)
			fout = new FileOutputStream(targetDir +"/TP");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.MRReq, 
					MetricType.TP), null, "MRRequest Group", "TP", true);
			fout.close();
		  	// AJWT
			fout = new FileOutputStream(targetDir +"/AJWT");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.MRReq, 
					MetricType.AJWT), null, "MRRequest Group", "AJWT", true);
			fout.close();
		  	// AJTT
			fout = new FileOutputStream(targetDir +"/AJTT");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.MRReq, 
					MetricType.AJTT), null, "MRRequest Group", "AJTT", true);
			fout.close();
		  	// AP
			fout = new FileOutputStream(targetDir +"/AP");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.MRReq, 
					MetricType.AP), null, "MRRequest Group", "AP", true);
			fout.close();
		  	// MUR
			fout = new FileOutputStream(targetDir +"/MUR");
			displayBarChart(fout, dataset.getCompDataset(wt, ParameterType.MRReq, 
					MetricType.MUR), null, "MRRequest Group", "MUR", true);
			fout.close();
		  */
		} catch (IOException e) {
			e.printStackTrace();
			if(fout != null) {
				try {
					fout.close();
				} catch (IOException e1) {}
			}
		}
	}
	
	public static void displayCDF(ZCDatasets dataset, String outputDir, WorkLoadType wt, int minDiffMB, int maxDiffMB) {
		FileOutputStream fout = null;
		String targetDir = null;
		File tdir = null;
		try {
			targetDir = outputDir + "/" + wt +"/predra";
			tdir = new File(targetDir);
			if(!tdir.exists()) tdir.mkdirs();
			fout = new FileOutputStream(targetDir +"/CDF_of_Default");
			displayLineChart(fout, dataset.getCDFDataset(wt, minDiffMB, maxDiffMB),
					null, "Predicted/real difference (MB)", "CDF", false);
			fout.close();
		} catch (IOException e) {
			e.printStackTrace();
			if(fout != null) {
				try {
					fout.close();
				} catch (IOException e1) {}
			}
		}
	}
	
	public static void displayCDF(ZCDatasets dataset, String outputDir, int minDiffMB, int maxDiffMB) {
		displayCDF(dataset, outputDir, WorkLoadType.SNS, minDiffMB, maxDiffMB);
		displayCDF(dataset, outputDir, WorkLoadType.SE, minDiffMB, maxDiffMB);
	}
	
	public static void displayAppStageTime(ZCDatasets dataset, String outputDir, String targetFullKey) {
		if(targetFullKey == null || targetFullKey.trim().equals("")) return;
		boolean displayAll = false;
		if(targetFullKey.equalsIgnoreCase("all")) displayAll = true;
		String targetDirStr = outputDir + "/appStageTimeCharts";
		File targetDir = new File(targetDirStr);
		if(!targetDir.exists()) targetDir.mkdirs();
		for(Entry<String, TreeSet<AppAttemptData>> fullKeyToOriDataset 
				: dataset.getAppDatasets().entrySet()) {
			String fullKey = fullKeyToOriDataset.getKey();
			String[] rectifiedFullKeys = fullKey.toLowerCase().split(" ");
			fullKey = rectifiedFullKeys[0];
			for(int i=1; i<rectifiedFullKeys.length; ++i) {
				fullKey += "_" + rectifiedFullKeys[i];
			}
			if(!displayAll && !targetFullKey.trim().equals(fullKey)) continue;
			String targetFile = targetDir  + "/" + fullKey + ".appastagetime";
			String targetDataFile = targetDir  + "/" + fullKey + ".appastagetime_data";
			DefaultCategoryDataset stageTimeDs = dataset.getAppAStageTimeDataset(fullKeyToOriDataset.getValue());
			OutputStream out = null;
			OutputStream outData = null;
			try {
				out = new FileOutputStream(targetFile);
				if(out != null) displayStackedBarChart(out, stageTimeDs, null, "appattemptid", "time", true);
				outData = new FileOutputStream(targetDataFile);
				if(outData != null) writeChartData(outData, stageTimeDs);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} finally{
				if(out != null)
					try {
						out.close();
					} catch (IOException e) {}
				  finally {
						if(outData != null)
							try {
								outData.close();
							} catch (IOException e) {}
				  }
			}
		}
	}
	
	public static void displayMetrics(ZCDatasets dataset, String outputDir) {
		displayEvaOfPredra(dataset, outputDir, WorkLoadType.SNS);
		displayEvaOfPredra(dataset, outputDir, WorkLoadType.SE);
		displayCompEva(dataset, outputDir, WorkLoadType.SNS);
		displayCompEva(dataset, outputDir, WorkLoadType.SE);
	}

	public static void main(String args[]) throws IOException {
		
		if(args.length < 3) {
			System.out.println("Usage: ZCCharts analyseResultDir $CONFDIR/parameters-display.conf chartsOutputDir [minDiffOfCDF|target_full_configure_to_display] [maxDiffOfCDF]");
			return;
		}
		int minDiffOfCDF = -500;
		int maxDiffOfCDF = 500;
		String workloadFileToDisplay = null;
		if(args.length == 4) {
			workloadFileToDisplay = args[3];
		} else if(args.length == 5) {
			minDiffOfCDF = Integer.parseInt(args[3]);
			maxDiffOfCDF = Integer.parseInt(args[4]);
		}
		ZCDatasets dataset = new ZCDatasets(args[0], args[1]);
		displayMetrics(dataset, args[2]);
		displayAppStageTime(dataset, args[2], workloadFileToDisplay); 
		displayCDF(dataset, args[2], minDiffOfCDF, maxDiffOfCDF);
	}
}

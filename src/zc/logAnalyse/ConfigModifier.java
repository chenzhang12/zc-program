package zc.logAnalyse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class ConfigModifier {

	private SAXReader saxReader = null;
	private Document document = null;
	private String destFile = null;

	public ConfigModifier(String sourceFile, String destFile) {
		saxReader = new SAXReader();
		try {
			document = saxReader.read(new File(sourceFile));
		} catch (DocumentException e) {
			e.printStackTrace();
			return;
		}
		this.destFile = destFile;
	}

	public void set(String propertyName, String propertyValue) {
		List<Element> list = document.selectNodes("/configuration/property");
		for (Element el : list) {
			List<Element> nameList = el.elements("name");
			List<Element> valueList = el.elements("value");
			if (nameList.size() > 0 && nameList.size() == valueList.size()) {
				if (nameList.get(0).getText().trim().equals(propertyName.trim())) {
					valueList.get(0).setText(propertyValue.trim());
					break;
				}
			}
		}
	}
	
	public void setInt(String propertyName, int propertyValue) {
		set(propertyName, String.valueOf(propertyValue));
	}
	
	public void setFloat(String propertyName, float propertyValue) {
		set(propertyName, String.valueOf(propertyValue));
	}

	/** 格式化输出,类型IE浏览一样 */
	public int format() {
		int returnValue = 0;
		try {

			XMLWriter writer = null;
			OutputFormat format = OutputFormat.createPrettyPrint();
			// /** 指定XML编码 */
			// format.setEncoding("GBK");
			writer = new XMLWriter(new FileWriter(new File(destFile)), format);
			writer.write(document);
			writer.close();
			/** 执行成功,需返回1 */
			returnValue = 1;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return returnValue;
	}

}

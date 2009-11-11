package com.shopping.hbase.imageserver;

import com.shopping.hbase.Utility;

public class FindDealId {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String dealId = ImportImages
				.getDealIdFromImageFilenameWithUnderScore(args[0] + ".jpg");
		System.out.println(dealId);
		// http://di1.shopping.com/images/di/65/59/55/717871516b5f7a706b4b4f5876573431747377-200x200-0-0.jpg
		
		double responseTime = Utility.getResponseTime("http://nym-qscs1.shopping.com:8080/images/di/2d/37/37/" + args[0]
				+ ".jpg");
		System.out.println("Retrieval time in milli seconds: "
				+ responseTime);

	}


}

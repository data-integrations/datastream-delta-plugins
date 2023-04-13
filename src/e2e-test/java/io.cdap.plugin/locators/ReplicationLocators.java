/*
 * Copyright (c) 2023.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;
/**
 * Oracle Plugin Locators.
 */
public class ReplicationLocators {
    @FindBy(how = How.XPATH, using = "//*[contains(text(),'Next')]")
    public static WebElement next;
    @FindBy(how = How.XPATH, using = "//div[contains(text(),'Oracle')]")
    public static WebElement oraclePlugin;
    public static WebElement selectTable(String tableName) {
        return SeleniumDriver.getDriver().findElement(By.xpath("//div[contains(text(),'" + tableName + "')]" +
                "/preceding-sibling::div/span"));
    }
    @FindBy(how = How.XPATH, using = "//span[contains(text(),'Deploy Replication Job')]")
    public static WebElement deployPipeline;
    @FindBy(how = How.XPATH, using = "//*[contains(text(), 'Running')]")
    public static WebElement running;
    @FindBy(how = How.XPATH, using = "//*[contains(text(), 'Logs')]")
    public static WebElement logs;
    @FindBy(how = How.XPATH, using = "(//*[contains(text(), 'View')])[1]")
    public static WebElement advancedLogs;
    @FindBy(how = How.XPATH, using = "//*[contains(@class, 'icon-stop')]")
    public static WebElement stop;
    @FindBy(how = How.XPATH, using = "//div[@data-cy='log-viewer-row']//div[contains(text(),'ERROR')]")
    public static WebElement error;
    @FindBy(how = How.XPATH, using = "//*[contains(text(),'Stopped')]")
    public static WebElement stopped;
    public static By start = By.xpath("//*[contains(@class, 'icon-play ')]");
}

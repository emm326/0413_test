using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using SolidWorks.Interop.sldworks;
using SolidWorks.Interop.swconst;

internal static partial class BuildNodeEnclosure
{
    private const double FaceHalfX = 47, FaceHalfY = 36, FaceRim = 71.4;
    private const double MainShelfZ = 50.4, MainBoardZ = MainShelfZ + ShelfT + BoardRest;
    private const double PowerShelfZ = 40, FacingPowerRest = 1.4;
    private const double PowerBoardZ = PowerShelfZ + ShelfT + FacingPowerRest;
    private const double FaceAntennaZ = MainBoardZ - 10.6 / 2;
    private const double SensorZ = 14, FaceGnssZ = 8.8;
    private const string FaceAssembly = "正面朝上双层传感器盒.SLDASM";
    private const string FaceReference = "正面朝上器件占位（不打印）";
    private static readonly string[] FaceNames =
    {
        "正面朝上盒体", "上层接口端支架", "上层螺栓端支架", "正面朝上压片", "电源板水平悬浮托板", "正面朝上盒盖"
    };
    private static readonly double[,] FaceLidHoles = { { -42.2, -31 }, { 42.2, -31 }, { -42.2, 31 }, { 42.2, 31 } };
    private static readonly double[,] MainEndMount = { { -38.4, -23 }, { -38.4, -.4 }, { 38.4, -23 }, { 38.4, -.4 } };
    private static readonly double[,] FaceClipHoles = { { -29.4, -30.2 }, { -29.4, 1.2 } };
    private static readonly double[,] PowerTrayMount = { { -39.5, 3.2 }, { 15, 3.2 }, { -39.5, 32.2 }, { 15, 32.2 } };
    private static readonly double[,] PowerBoardMount = { { -36.75, 8.9 }, { 13.25, 8.9 }, { -36.75, 25.9 }, { 13.25, 25.9 } };
    private static readonly double[,] FaceGnssHoles = { { 21.1, 3.4 }, { 41.6, 3.4 } };
    private static readonly double[,] FaceSensorHoles = { { -32, 27.5 }, { -13, 27.5 } };

    private static int BuildFaceUp(string folder, bool assembleOnly = false)
    {
        output = Path.GetFullPath(folder);
        Directory.CreateDirectory(output);
        Require(assembleOnly || !Directory.EnumerateFileSystemEntries(output).Any(), "Output directory is not empty.");
        Require(!File.Exists(Path.Combine(output, FaceAssembly)), "Face-up assembly already exists.");
        sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
        var parts = assembleOnly ? FaceNames.Select(LoadPart).ToArray() : new[]
        {
            FaceBase(), FaceEnd(true), FaceEnd(false), DualClip(FaceNames[3]), FacePowerTray(), FaceLid()
        };
        var reference = assembleOnly ? LoadPart(FaceReference) : FaceReferences();
        ExportPrintableParts(parts);
        var doc = NewDoc(true);
        var asm = (AssemblyDoc)doc;
        doc.IGetActiveConfiguration().Name = DualClosed;
        PlaceDual(doc, parts[0], XY, 0, 0, 0);
        PlaceDual(doc, parts[1], XY, 0, 0, MainShelfZ);
        PlaceDual(doc, parts[2], XY, 0, 0, MainShelfZ);
        for (int i = 0; i < 2; i++)
            PlaceDual(doc, parts[3], i == 0 ? XY : HalfTurn, FaceClipHoles[i, 0], FaceClipHoles[i, 1], MainShelfZ + ShelfT);
        PlaceDual(doc, parts[4], XY, 0, 0, PowerShelfZ);
        PlaceDual(doc, parts[5], XY, 0, 0, FaceRim);
        PlaceDual(doc, reference, XY, 0, 0, 0);
        CheckDualState(doc, 8, true);
        AddDualConfig(doc, DualLayout, "Lid removed; all electronic envelopes face up");
        ActivateConfiguration(doc, DualClosed);
        AddDualConfig(doc, DualService, "Lid and the three removable PCB supports removed");
        foreach (string name in FaceNames.Skip(1).Concat(new[] { FaceReference }))
            foreach (var item in PartComponents(asm, name)) SetDualSuppression(doc, item);
        ActivateConfiguration(doc, DualClosed);
        SetDualSuppression(doc, PartComponents(asm, FaceReference).Single());
        ActivateConfiguration(doc, DualLayout);
        SetDualSuppression(doc, PartComponents(asm, FaceNames[5]).Single());
        CheckFaceAssembly(doc);
        ActivateConfiguration(doc, DualLayout);
        View(doc);
        Save(doc, FaceAssembly);
        sw.CloseDoc(doc.GetTitle());
        sw.CloseDoc(reference.GetTitle());
        foreach (var part in parts) sw.CloseDoc(part.GetTitle());
        int err = 0, warn = 0;
        var path = Path.Combine(output, FaceAssembly);
        doc = sw.OpenDoc6(path, 2, 1, DualLayout, ref err, ref warn);
        Require(doc != null && err == 0 && (warn & ~(int)swFileLoadWarning_e.swFileLoadWarning_NeedsRegen) == 0,
            "Reopen face-up assembly: " + err + "/" + warn);
        foreach (var item in ((object[])((AssemblyDoc)doc).GetComponents(true)).Cast<Component2>())
        {
            string found = item.GetPathName();
            Require(string.Equals(Path.GetDirectoryName(found), output, StringComparison.OrdinalIgnoreCase) && File.Exists(found),
                "Unresolved or nonlocal component: " + found);
        }
        CheckFaceAssembly(doc);
        ActivateConfiguration(doc, DualLayout);
        View(doc);
        Require(doc.Save3((int)swSaveAsOptions_e.swSaveAsOptions_Silent, ref err, ref warn) && err == 0 && warn == 0,
            "Save verified face-up assembly: " + err + "/" + warn);
        Console.WriteLine("Verified and open: " + path);
        return 0;
    }

    private static void CheckFaceAssembly(ModelDoc2 doc)
    {
        ActivateConfiguration(doc, DualClosed);
        CheckDualState(doc, 7, true);
        var lid = PartComponents((AssemblyDoc)doc, FaceNames[5]).Single();
        Near(((double[])lid.Transform2.ArrayData)[11] * 1000 + DualLidT, 74, "Assembly height");
        ActivateConfiguration(doc, DualLayout);
        CheckDualState(doc, 7, true);
        ActivateConfiguration(doc, DualService);
        CheckDualState(doc, 1, true);
    }

    private static ModelDoc2 FaceBase()
    {
        var doc = NewDoc(false);
        Add(doc, "盒底及贯穿安装孔", 0, DualFloor, s =>
        {
            Outline(s, FaceHalfX, FaceHalfY, 2);
            Circles(s, FaceGnssHoles, 1.3);
            Circles(s, FaceSensorHoles, 1.1);
            Circles(s, new double[,] { { -41, -10 }, { 41, -10 }, { -42, 15 }, { 42, 15 } }, 2.1);
            Slot(s, 1, -32.8, 7, 1.4);
            Slot(s, 1, 2.1, 7, 1.8);
        });
        Add(doc, "2.4 mm周壁", DualFloor, FaceRim - DualFloor, s =>
        {
            Outline(s, FaceHalfX, FaceHalfY, 2);
            Outline(s, FaceHalfX - 2.4, FaceHalfY - 2.4, .6);
        });
        Add(doc, "盒盖上部连接耳 Ø2.2导孔", 66.9, FaceRim - 66.9, s =>
        {
            foreach (var x in new[] { -1d, 1d })
                foreach (var y in new[] { -1d, 1d })
                    Rect(s, Math.Min(x * 39.9, x * 44.2), Math.Min(y * 28.8, y * 34.1),
                        Math.Max(x * 39.9, x * 44.2), Math.Max(y * 28.8, y * 34.1));
            Circles(s, FaceLidHoles, 1.1);
        });
        Add(doc, "电池垫条", DualFloor, BatteryRest, s =>
        {
            Rect(s, -28, -31.8, -23, -.3);
            Rect(s, 23, -31.8, 28, -.3);
        });
        Add(doc, "电池端挡", DualFloor, 3, s =>
        {
            Rect(s, -36.5, -31.4, -34.9, -28);
            Rect(s, 34.9, -31.4, 36.5, -28);
        });
        Add(doc, "传感器无排针侧支柱 Ø2.2 贯穿", DualFloor, SensorZ - DualFloor, s =>
        {
            Circles(s, FaceSensorHoles, 2.7);
            Circles(s, FaceSensorHoles, 1.1);
        });
        Add(doc, "GNSS单边凸台 Ø2.6 贯穿，板内接触3mm", DualFloor, FaceGnssZ - DualFloor, s =>
        {
            Polygon(s, new double[,] { { 18.45, 1.2 }, { 44.2, 1.2 }, { 44.2, 5 }, { 38.9, 5 }, { 38.9, 2 },
                { 23.8, 2 }, { 23.8, 5 }, { 18.45, 5 } });
            Circles(s, FaceGnssHoles, 1.3);
        });
        Add(doc, "上层端部支座根部", DualFloor, MainShelfZ - DualFloor - 6, s => FaceMainPosts(s, false));
        Add(doc, "上层端部支座 Ø1.65导孔", MainShelfZ - 6, 6, s => FaceMainPosts(s, true));
        Add(doc, "水平电源托板支柱根部", DualFloor, PowerShelfZ - DualFloor - 6, s => FacePowerPosts(s, false));
        Add(doc, "水平电源托板 Ø1.65导孔", PowerShelfZ - 6, 6, s => FacePowerPosts(s, true));
        DualPort(doc, "Type-C 暂定10×11开口", 61, 11, 10, s => Rect(s, -47.3, -14, -44.2, -4));
        var start = VolumeMm3(doc);
        const string port = "反面天线连接位置 Ø7预留孔";
        FaceCircle(doc, NegativeYFace(Solids(doc).Single(), -FaceHalfY), port, AntennaX, -FaceHalfY, FaceAntennaZ, 3.5);
        var cut = doc.FeatureManager.FeatureCut4(true, false, false, 0, 0, M(2.5), 0,
            false, false, false, false, 0, 0, false, false, false, false,
            false, true, true, false, false, false, 0, 0, false, false);
        CheckFeature(cut, port);
        Near(start - VolumeMm3(doc), Math.PI * 3.5 * 3.5 * 2.4, "Round side port");
        VerifyPart(doc, 94, 72, FaceRim);
        return SavePart(doc, FaceNames[0], new[] { .24, .30, .34 });
    }

    private static void FaceMainPosts(SketchManager s, bool holes)
    {
        foreach (double y in new[] { -23d, -.4 })
        {
            Rect(s, 36.3, y - 1.9, 44.9, y + 1.9);
            Rect(s, -44.9, y - 1.9, -36.3, y + 1.9);
        }
        if (holes) Circles(s, MainEndMount, .825);
    }

    private static void FacePowerPosts(SketchManager s, bool holes)
    {
        foreach (double x in new[] { -39.5, 15 })
        {
            Rect(s, x - 2.15, 1.45, x + 2.15, 4.6);
            Rect(s, x - 2.15, 30.6, x + 2.15, 34.0);
        }
        if (holes) Circles(s, PowerTrayMount, .825);
    }

    private static void FaceBoltSlots(SketchManager s)
    {
        foreach (double y in new[] { -25.5, -3.5 }) Slot(s, 30.1, y, 5.4, 2.8);
    }

    private static ModelDoc2 FaceEnd(bool usb)
    {
        var doc = NewDoc(false);
        Add(doc, usb ? "接口端底座，Type-C区开口" : "螺栓端底座，背面器件避空", 0, ShelfT, s =>
        {
            if (usb)
            {
                Polygon(s, new double[,] { { -40.5, -32 }, { -25.2, -32 }, { -25.2, -14 }, { -35, -14 },
                    { -35, -4 }, { -25.2, -4 }, { -25.2, 3.15 }, { -40.5, 3.15 } });
                Circles(s, new double[,] { { -38.4, -23 }, { -38.4, -.4 } }, 1.15);
                Circles(s, FaceClipHoles, 1.15);
            }
            else
            {
                Polygon(s, new double[,] { { 25.7, -32 }, { 40.5, -32 }, { 40.5, 3.15 }, { 25.7, 3.15 },
                    { 25.7, -7.4 }, { 36.4, -7.4 }, { 36.4, -21.6 }, { 25.7, -21.6 } });
                Circles(s, new double[,] { { 38.4, -23 }, { 38.4, -.4 } }, 1.15);
                FaceBoltSlots(s);
            }
        });
        Add(doc, "只托住开发板两端的边缘", ShelfT, BoardRest, s =>
        {
            if (usb)
            {
                Rect(s, -32, -27.8, -26.3, -25);
                Rect(s, -32, -3.6, -26.3, -1.2);
            }
            else
            {
                Rect(s, 26.9, -27.7, 33.4, -23.3);
                Rect(s, 26.9, -5.7, 33.4, -1.3);
                FaceBoltSlots(s);
            }
        });
        VerifyPart(doc, usb ? 15.3 : 14.8, 35.15, ShelfT + BoardRest);
        return SavePart(doc, FaceNames[usb ? 1 : 2], new[] { .22, .52, .50 });
    }

    private static ModelDoc2 FacePowerTray()
    {
        var doc = NewDoc(false);
        Add(doc, "水平悬浮托板及四个可拆安装孔", 0, ShelfT, s =>
        {
            Polygon(s, new double[,] { { -43.1, 4 }, { -41.7, 4 }, { -41.7, 1.65 }, { -37.3, 1.65 }, { -37.3, 4 },
                { 12.8, 4 }, { 12.8, 1.65 }, { 17.2, 1.65 }, { 17.2, 4 }, { 17.8, 4 }, { 17.8, 30.75 },
                { 17.2, 30.75 }, { 17.2, 33.5 }, { 12.8, 33.5 }, { 12.8, 30.75 }, { -37.3, 30.75 },
                { -37.3, 33.5 }, { -41.7, 33.5 }, { -41.7, 30.75 }, { -43.1, 30.75 } });
            Circles(s, PowerTrayMount, 1.15);
            Circles(s, PowerBoardMount, 1.5);
        });
        Add(doc, "电源板背面焊脚垫空", ShelfT, FacingPowerRest, s =>
        {
            Circles(s, PowerBoardMount, 2.65);
            Circles(s, PowerBoardMount, 1.5);
        });
        VerifyPart(doc, 60.9, 31.85, ShelfT + FacingPowerRest);
        return SavePart(doc, FaceNames[4], new[] { .23, .46, .57 });
    }

    private static ModelDoc2 FaceLid()
    {
        var doc = NewDoc(false);
        Add(doc, "平盖和四个 Ø2.8 通孔", 0, DualLidT, s =>
        {
            Outline(s, FaceHalfX, FaceHalfY, 2);
            Circles(s, FaceLidHoles, 1.4);
        });
        VerifyPart(doc, 94, 72, DualLidT);
        return SavePart(doc, FaceNames[5], new[] { .32, .39, .42 });
    }

    private static ModelDoc2 FaceReferences()
    {
        var doc = NewDoc(false);
        ReferenceBox(doc, "电池盒", -34.5, -33, DualFloor + BatteryRest, 34.5, 1, DualFloor + BatteryRest + 18, .18, .20, .23);
        ReferenceShape(doc, "传感器板 24×24，Ø2.2孔", SensorZ, SensorZ + 1.6, s =>
        {
            Rect(s, -34.5, 6, -10.5, 30);
            Circles(s, FaceSensorHoles, 1.1);
        }, .12, .49, .38);
        ReferenceBox(doc, "排针侧5mm，板下按10mm避空", -34.5, 6, SensorZ - 10, -10.5, 11, SensorZ, .28, .30, .36);
        ReferenceBox(doc, "排针侧5mm，板上线头和弯线预留", -34.5, 6, SensorZ + 1.6, -10.5, 11, SensorZ + 1.6 + 23, .22, .25, .31);
        ReferenceShape(doc, "GNSS板，3mm单边Ø2.6安装区", FaceGnssZ, FaceGnssZ + 1.6, s =>
        {
            Rect(s, 18.5, 2, 44, 32.5);
            Circles(s, FaceGnssHoles, 1.3);
        }, .14, .39, .74);
        ReferenceBox(doc, "GNSS陶瓷天线面朝上，保留11.5mm", 20.0, 5.15, FaceGnssZ + 1.6, 42.5, 31.2, FaceGnssZ + 1.6 + 11.5, .83, .69, .43);
        ReferenceBox(doc, "GNSS背面插头预留", 26, 2.05, FaceGnssZ - 4.5, 34, 8.5, FaceGnssZ, .43, .50, .64);
        ReferenceShape(doc, "电源板正面朝上", PowerBoardZ, PowerBoardZ + 1.6, s =>
        {
            Rect(s, -41.5, 4.5, 18, 30.3);
            Circles(s, PowerBoardMount, 1.5);
        }, .27, .34, .46);
        ReferenceBox(doc, "电源板最高件20mm净空", -41.5, 4.5, PowerBoardZ + 1.6, 18, 30.3, PowerBoardZ + 1.6 + 20, .29, .33, .41);
        ReferenceBox(doc, "电源板插线预留超过20mm的部分", -28, 10, PowerBoardZ + 1.6 + 20, 4, 19, PowerBoardZ + 1.6 + 22, .23, .26, .32);
        ReferenceShape(doc, "EoRa屏幕面朝上的开发板及端部凸起", MainBoardZ, MainBoardZ + BoardT, s =>
        {
            Polygon(s, new double[,] { { -32, -28 }, { 32, -28 }, { 32, -17.25 }, { 35, -17.25 },
                { 35, -11.75 }, { 32, -11.75 }, { 32, -1 }, { -32, -1 } });
        }, .12, .52, .43);
        ReferenceBox(doc, "EoRa屏幕5.5mm", -8.5, -23.5, MainBoardZ + BoardT, 25, -6, MainBoardZ + BoardT + 5.5, .19, .24, .30);
        ReferenceBox(doc, "EoRa背面排针和杜邦线通道甲", -10.5, -27.7, MainBoardZ - 23, 25.2, -24.3, MainBoardZ, .21, .24, .29);
        ReferenceBox(doc, "EoRa背面排针和杜邦线通道乙", -10.5, -4.6, MainBoardZ - 23, 25.2, -1.2, MainBoardZ, .21, .24, .29);
        ReferenceBox(doc, "EoRa接口端背面4mm避空", -34, -13.6, MainBoardZ - 4, -27, -4.3, MainBoardZ, .41, .44, .49);
        ReferenceBox(doc, "EoRa另一端背面5mm避空", 27, -21.1, MainBoardZ - 5, 35, -7.9, MainBoardZ, .41, .44, .49);
        Require(doc.ForceRebuild3(false) && Solids(doc).Length == 16, "Unexpected face-up envelope count.");
        View(doc);
        Save(doc, FaceReference + ".SLDPRT");
        return doc;
    }
}

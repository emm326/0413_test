clear; clc; close all;

%% File paths
txtFile = 'C:\Users\26223\Desktop\wxd测量\260620\070514147\25_1_2.txt';
csvFile = 'C:\Users\26223\Desktop\zawu\testesp32\0413_test\data_mems1\sensor1001\20260705\tx_usb_capacity.csv';

%% User settings
g0 = 9.80665;

% CAYD data unit:
%   If the CAYD TXT is already m/s^2, keep 1.
%   If the CAYD TXT is g, change to g0.
%   If the CAYD TXT is voltage, change this using the sensor/amplifier sensitivity.
caydScaleToMps2 = 1;

targetFs = 50;                 % CAYD is about 50 Hz; resample IIM to this grid
bandHz = [0.5 20];             % valid for 50 Hz data; keep below 25 Hz Nyquist
useBandpass = true;
maxLagSeconds = 2.0;           % local lag search range
eventHalfWindowSeconds = 2.0;  % plot +- this many seconds around the strongest event

%% Read CAYD-188 TXT
% Force the timestamp column to text. Otherwise readtable may auto-convert it
% to datetime and silently round 4 fractional digits to milliseconds.
D = readtable(txtFile, ...
    'FileType', 'text', ...
    'Delimiter', ',', ...
    'ReadVariableNames', false, ...
    'TextType', 'string', ...
    'Format', '%s%f');
tCayd = datetime(D.Var1, 'InputFormat', 'yyyy-MM-dd HH:mm:ss.SSSS');
aCayd = D.Var2 * caydScaleToMps2;
[tCayd, aCayd] = cleanSeries(tCayd, aCayd);

%% Read IIM-42352 CSV
% Same reason here: preserve timestamps such as 22:10:32.0382 exactly.
S = readtable(csvFile, ...
    'FileType', 'text', ...
    'Delimiter', ',', ...
    'TextType', 'string', ...
    'Format', '%s%f%f%f');
tIim = datetime(S.datetime, 'InputFormat', 'yyyy/M/d HH:mm:ss.SSSS');
aIim = [S.ax, S.ay, S.az] * g0;   % IIM CSV is in g; convert to m/s^2
[tIim, aIim] = cleanMatrixSeries(tIim, aIim);
axisNames = ["ax", "ay", "az"];

%% Common time span and common 50 Hz grid
tStart = max([min(tCayd), min(tIim)]);
tEnd = min([max(tCayd), max(tIim)]);
if tStart >= tEnd
    error('The two files have no common time range. Please check the timestamps.');
end

nGrid = floor(seconds(tEnd - tStart) * targetFs) + 1;
gridSeconds = (0:nGrid-1)' / targetFs;
tGrid = tStart + seconds(gridSeconds);

tc = seconds(tCayd - tStart);
ti = seconds(tIim - tStart);

caydRaw = interp1(tc, aCayd, gridSeconds, 'linear');
iimRaw = interp1(ti, aIim, gridSeconds, 'linear');

%% Remove DC/gravity/zero offset on the same time grid
caydDyn = caydRaw - mean(caydRaw, 'omitnan');
iimDyn = iimRaw - mean(iimRaw, 1, 'omitnan');

%% Apply the same filter to both systems
[caydUse, filterNote] = maybeBandpass(caydDyn, targetFs, bandHz, useBandpass);
[iimUse, ~] = maybeBandpass(iimDyn, targetFs, bandHz, useBandpass);

%% Pick one local event; long sparse impulse plots are not a fair time-domain check
halfN = max(1, round(eventHalfWindowSeconds * targetFs));
eventSearch = abs(caydUse);
eventSearch(1:min(halfN, numel(eventSearch))) = NaN;
eventSearch(max(1, numel(eventSearch)-halfN+1):end) = NaN;
[eventValue, eventIdx] = max(eventSearch, [], 'omitnan');
if isnan(eventValue) || eventIdx < 1
    [~, eventIdx] = max(abs(caydUse));
end

winIdx = max(1, eventIdx-halfN) : min(nGrid, eventIdx+halfN);

%% Find the IIM axis, polarity, and lag that best match CAYD in this event window
maxLagSamples = round(maxLagSeconds * targetFs);
best = struct('axis', 1, 'corr', -Inf, 'shiftSamples', 0, 'polarity', 1);

for k = 1:3
    [rho, shiftSamples, polarity] = bestShiftCorr(caydUse(winIdx), iimUse(winIdx, k), maxLagSamples);
    if abs(rho) > abs(best.corr) || isinf(best.corr)
        best.axis = k;
        best.corr = rho;
        best.shiftSamples = shiftSamples;
        best.polarity = polarity;
    end
end

iimBest = best.polarity * iimUse(:, best.axis);
iimAligned = shiftWithNan(iimBest, best.shiftSamples);

validLocal = winIdx(isfinite(caydUse(winIdx)) & isfinite(iimAligned(winIdx)));
scaleRms = rmsNoNan(caydUse(validLocal)) / max(rmsNoNan(iimAligned(validLocal)), eps);
iimScaled = iimAligned * scaleRms;

localCorr = corrNoToolbox(caydUse(validLocal), iimAligned(validLocal));
localCorrScaled = corrNoToolbox(caydUse(validLocal), iimScaled(validLocal));

%% Print metrics
fsCaydEst = 1 / median(seconds(diff(tCayd)));
fsIimEst = 1 / median(seconds(diff(tIim)));
[~, txtBase, ~] = fileparts(txtFile);
[~, csvBase, ~] = fileparts(csvFile);

fprintf('\n===== CAYD vs IIM aligned comparison =====\n');
fprintf('CAYD file: %s\n', txtBase);
fprintf('IIM file : %s\n', csvBase);
fprintf('Common time: %s to %s\n', char(tStart), char(tEnd));
fprintf('Estimated sample rate: CAYD %.4f Hz, IIM %.4f Hz, common grid %.4f Hz\n', fsCaydEst, fsIimEst, targetFs);
fprintf('Filter: %s\n', filterNote);
fprintf('Selected event center: %s\n', char(tGrid(eventIdx)));
fprintf('Best IIM axis: %s\n', axisNames(best.axis));
fprintf('Polarity applied to IIM: %+d\n', best.polarity);
fprintf('IIM alignment shift: %+d samples = %+0.4f s\n', best.shiftSamples, best.shiftSamples / targetFs);
fprintf('Local waveform correlation before RMS scaling: %.4f\n', localCorr);
fprintf('Local waveform correlation after RMS scaling : %.4f\n', localCorrScaled);
fprintf('RMS amplitude scale, CAYD/IIM: %.4f\n', scaleRms);
fprintf('Note: scaling is for shape comparison only; physical amplitude difference is shown in the unscaled plot.\n\n');

%% Figure 1: overview after same preprocessing, before alignment
figure('Color', 'w', 'Name', 'Overview before alignment');
plot(tGrid, caydUse, 'k', 'LineWidth', 1); hold on;
plot(tGrid, iimUse(:,1), 'r', 'LineWidth', 0.8);
plot(tGrid, iimUse(:,2), 'g', 'LineWidth', 0.8);
plot(tGrid, iimUse(:,3), 'b', 'LineWidth', 0.8);
xline(tGrid(eventIdx), '--m', 'Event');
grid on;
xlabel('Time');
ylabel('Acceleration (m/s^2)');
title('Overview after common sampling, de-meaning, and common filtering; before time shift');
legend('CAYD', 'IIM ax', 'IIM ay', 'IIM az', 'Location', 'best');

%% Figure 2: local event, aligned best axis
figure('Color', 'w', 'Name', 'Aligned local event comparison');

subplot(3,1,1);
plot(tGrid(winIdx), caydUse(winIdx), 'k', 'LineWidth', 1.2); hold on;
plot(tGrid(winIdx), iimAligned(winIdx), 'r', 'LineWidth', 1.0);
grid on;
ylabel('m/s^2');
title(sprintf('Local event, real amplitude: CAYD vs IIM %s; aligned, not amplitude-scaled', axisNames(best.axis)));
legend('CAYD', sprintf('IIM %s aligned', axisNames(best.axis)), 'Location', 'best');

subplot(3,1,2);
plot(tGrid(winIdx), caydUse(winIdx), 'k', 'LineWidth', 1.2); hold on;
plot(tGrid(winIdx), iimScaled(winIdx), 'r', 'LineWidth', 1.0);
grid on;
ylabel('m/s^2');
title(sprintf('Local event, RMS amplitude matched: corr %.3f, CAYD/IIM scale = %.3f', localCorrScaled, scaleRms));
legend('CAYD', 'IIM aligned and RMS-scaled', 'Location', 'best');

subplot(3,1,3);
caydNorm = normalizeMaxAbs(caydUse(winIdx));
iimNorm = normalizeMaxAbs(iimAligned(winIdx));
plot(tGrid(winIdx), caydNorm, 'k', 'LineWidth', 1.2); hold on;
plot(tGrid(winIdx), iimNorm, 'r', 'LineWidth', 1.0);
grid on;
xlabel('Time');
ylabel('Normalized');
title('Local event normalized waveform: shape and phase only');
legend('CAYD normalized', 'IIM normalized', 'Location', 'best');

%% Figure 3: local FFT shape comparison
figure('Color', 'w', 'Name', 'Local frequency comparison');
[fC, ampC] = singleSidedFft(caydUse(winIdx), targetFs);
[fI, ampI] = singleSidedFft(iimAligned(winIdx), targetFs);
plot(fC, normalizeMaxAbs(ampC), 'k', 'LineWidth', 1.2); hold on;
plot(fI, normalizeMaxAbs(ampI), 'r', 'LineWidth', 1.0);
grid on;
xlim([0, min(25, targetFs/2)]);
xlabel('Frequency (Hz)');
ylabel('Normalized amplitude');
title('Local event frequency-shape comparison');
legend('CAYD FFT', sprintf('IIM %s FFT', axisNames(best.axis)), 'Location', 'best');

%% Local helper functions
function [t, x] = cleanSeries(t, x)
    keep = isfinite(x) & ~isnat(t);
    t = t(keep);
    x = x(keep);
    [t, order] = sort(t);
    x = x(order);
end

function [t, x] = cleanMatrixSeries(t, x)
    keep = all(isfinite(x), 2) & ~isnat(t);
    t = t(keep);
    x = x(keep, :);
    [t, order] = sort(t);
    x = x(order, :);
end

function [y, note] = maybeBandpass(x, fs, bandHz, useBandpass)
    y = x;
    note = 'no filter';
    if ~useBandpass
        return;
    end

    lowHz = max(bandHz(1), 0.001);
    highHz = min(bandHz(2), 0.45 * fs);
    if highHz <= lowHz || highHz >= fs / 2
        warning('Bandpass range is invalid for fs=%.3f Hz; filter skipped.', fs);
        note = 'filter skipped because band is invalid';
        return;
    end

    if exist('butter', 'file') == 2 && exist('filtfilt', 'file') == 2
        x = fillMissingLinear(x);
        [b, a] = butter(4, [lowHz, highHz] / (fs / 2), 'bandpass');
        y = filtfilt(b, a, x);
        note = sprintf('4th-order zero-phase bandpass %.2f-%.2f Hz', lowHz, highHz);
    else
        warning('butter/filtfilt not found; install Signal Processing Toolbox or set useBandpass=false.');
        note = 'filter skipped because Signal Processing Toolbox functions were not found';
    end
end

function x = fillMissingLinear(x)
    n = size(x, 1);
    q = (1:n)';
    for col = 1:size(x, 2)
        good = isfinite(x(:, col));
        if all(good)
            continue;
        end
        if nnz(good) < 2
            x(:, col) = 0;
        else
            x(:, col) = interp1(q(good), x(good, col), q, 'linear', 'extrap');
        end
    end
end

function [bestRho, bestShift, polarity] = bestShiftCorr(ref, sig, maxLag)
    bestRho = 0;
    bestShift = 0;
    for shift = -maxLag:maxLag
        shifted = shiftWithNan(sig, shift);
        valid = isfinite(ref) & isfinite(shifted);
        if nnz(valid) < 5
            continue;
        end
        rho = corrNoToolbox(ref(valid), shifted(valid));
        if abs(rho) > abs(bestRho)
            bestRho = rho;
            bestShift = shift;
        end
    end
    polarity = sign(bestRho);
    if polarity == 0
        polarity = 1;
    end
end

function y = shiftWithNan(x, shiftSamples)
    y = NaN(size(x));
    n = numel(x);
    if shiftSamples > 0
        y((1+shiftSamples):n) = x(1:(n-shiftSamples));
    elseif shiftSamples < 0
        s = -shiftSamples;
        y(1:(n-s)) = x((1+s):n);
    else
        y = x;
    end
end

function rho = corrNoToolbox(x, y)
    x = x(:);
    y = y(:);
    good = isfinite(x) & isfinite(y);
    x = x(good);
    y = y(good);
    if numel(x) < 5
        rho = NaN;
        return;
    end
    x = x - mean(x);
    y = y - mean(y);
    den = sqrt(sum(x.^2) * sum(y.^2));
    if den <= eps
        rho = NaN;
    else
        rho = sum(x .* y) / den;
    end
end

function r = rmsNoNan(x)
    x = x(isfinite(x));
    if isempty(x)
        r = NaN;
    else
        r = sqrt(mean(x.^2));
    end
end

function y = normalizeMaxAbs(x)
    m = max(abs(x), [], 'omitnan');
    if isempty(m) || ~isfinite(m) || m <= eps
        y = x;
    else
        y = x / m;
    end
end

function [f, amp] = singleSidedFft(x, fs)
    x = x(:);
    x = x(isfinite(x));
    x = x - mean(x);
    n = numel(x);
    if n < 2
        f = 0;
        amp = 0;
        return;
    end
    w = hannLocal(n);
    x = x .* w;
    y = fft(x);
    p2 = abs(y / n);
    n1 = floor(n / 2) + 1;
    amp = p2(1:n1);
    if numel(amp) > 2
        amp(2:end-1) = 2 * amp(2:end-1);
    end
    f = fs * (0:(n1-1))' / n;
end

function w = hannLocal(n)
    if n <= 1
        w = 1;
    else
        k = (0:n-1)';
        w = 0.5 - 0.5 * cos(2 * pi * k / (n - 1));
    end
end

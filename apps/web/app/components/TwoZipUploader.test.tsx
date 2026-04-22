import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi } from 'vitest';

import TwoZipUploader from './TwoZipUploader';

function makeFile(name: string, sizeBytes: number, type = 'application/zip'): File {
  return new File([new Uint8Array(sizeBytes)], name, { type });
}

describe('TwoZipUploader', () => {
  it('renders both pickers', () => {
    render(
      <TwoZipUploader
        schema={null} source={null}
        onSchema={() => {}} onSource={() => {}}
      />
    );
    expect(screen.getByTestId('schema-picker')).toBeInTheDocument();
    expect(screen.getByTestId('source-picker')).toBeInTheDocument();
  });

  it('accepts a .zip and reports up', async () => {
    const onSchema = vi.fn();
    render(
      <TwoZipUploader
        schema={null} source={null}
        onSchema={onSchema} onSource={() => {}}
      />
    );
    const file = makeFile('schema.zip', 100);
    const input = screen.getByTestId('schema-picker-input') as HTMLInputElement;
    await userEvent.upload(input, file);
    expect(onSchema).toHaveBeenCalledTimes(1);
    expect(onSchema.mock.calls[0][0].name).toBe('schema.zip');
  });

  it('rejects non-zip extensions', async () => {
    const onSchema = vi.fn();
    render(
      <TwoZipUploader
        schema={null} source={null}
        onSchema={onSchema} onSource={() => {}}
      />
    );
    const file = makeFile('schema.txt', 100, 'text/plain');
    const input = screen.getByTestId('schema-picker-input') as HTMLInputElement;
    await userEvent.upload(input, file);
    expect(onSchema).not.toHaveBeenCalled();
    expect(screen.getByTestId('schema-picker-error')).toHaveTextContent(/must be a \.zip/);
  });

  it('rejects oversized zips', async () => {
    const onSchema = vi.fn();
    render(
      <TwoZipUploader
        schema={null} source={null}
        onSchema={onSchema} onSource={() => {}}
      />
    );
    // 101 MB -> over the 100 MB cap.
    const file = makeFile('big.zip', 101 * 1024 * 1024);
    const input = screen.getByTestId('schema-picker-input') as HTMLInputElement;
    await userEvent.upload(input, file);
    expect(onSchema).not.toHaveBeenCalled();
    expect(screen.getByTestId('schema-picker-error')).toHaveTextContent(/exceeds 100 MB/);
  });

  it('disables interaction when disabled', () => {
    render(
      <TwoZipUploader
        schema={null} source={null}
        onSchema={() => {}} onSource={() => {}}
        disabled
      />
    );
    const input = screen.getByTestId('schema-picker-input') as HTMLInputElement;
    expect(input).toBeDisabled();
  });

  it('shows clear button when file present and resets via parent', async () => {
    const onSchema = vi.fn();
    const file = makeFile('s.zip', 50);
    render(
      <TwoZipUploader
        schema={file} source={null}
        onSchema={onSchema} onSource={() => {}}
      />
    );
    expect(screen.getByText('s.zip')).toBeInTheDocument();
    await userEvent.click(screen.getByText('Clear'));
    expect(onSchema).toHaveBeenCalledWith(null);
  });
});
